#!/usr/bin/env node
"use strict";

const crypto = require("crypto");
const fs = require("fs");
const https = require("https");
const path = require("path");

function log(message) {
  console.log(`[cefas] ${message}`);
}

function fail(message) {
  console.error(`[cefas] error: ${message}`);
  process.exit(1);
}

function goosFromPlatform(platform) {
  switch (platform) {
    case "darwin":
      return "darwin";
    case "linux":
      return "linux";
    case "win32":
      return "windows";
    default:
      return null;
  }
}

function goarchFromArch(arch) {
  switch (arch) {
    case "x64":
      return "amd64";
    case "arm64":
      return "arm64";
    default:
      return null;
  }
}

function githubToken() {
  return (
    process.env.CEFAS_GITHUB_TOKEN ||
    process.env.GITHUB_TOKEN ||
    process.env.GH_TOKEN ||
    ""
  ).trim();
}

function githubHeaders(extraHeaders) {
  const headers = {
    "User-Agent": "@cefasdb/cefas postinstall",
    ...extraHeaders,
  };
  const token = githubToken();
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }
  return headers;
}

function request(url, headers, redirectsLeft) {
  return new Promise((resolve, reject) => {
    const req = https.request(url, { headers }, (res) => {
      const code = res.statusCode || 0;
      const location = res.headers.location;

      if (code >= 300 && code < 400 && location && redirectsLeft > 0) {
        const next = location.startsWith("http")
          ? location
          : new URL(location, url).toString();
        res.resume();
        resolve(request(next, headers, redirectsLeft - 1));
        return;
      }

      if (code < 200 || code >= 300) {
        let body = "";
        res.setEncoding("utf8");
        res.on("data", (chunk) => {
          body += chunk;
        });
        res.on("end", () => {
          reject(new Error(`HTTP ${code} for ${url}${body ? `: ${body.slice(0, 200)}` : ""}`));
        });
        return;
      }

      resolve(res);
    });

    req.on("error", reject);
    req.end();
  });
}

async function downloadText(url) {
  const res = await request(url, githubHeaders({ Accept: "*/*" }), 5);
  return new Promise((resolve, reject) => {
    let out = "";
    res.setEncoding("utf8");
    res.on("data", (chunk) => {
      out += chunk;
    });
    res.on("end", () => resolve(out));
    res.on("error", reject);
  });
}

function parseExpectedSha256(sumText, assetName) {
  for (const line of sumText.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    const parts = trimmed.split(/\s+/);
    if (parts.length < 2) continue;

    const sha = parts[0].toLowerCase();
    const file = parts[1].replace(/^dist\//, "");
    if (file === assetName) {
      return sha;
    }
  }
  return null;
}

async function downloadFileWithSha256(url, outPath, expectedSha256) {
  if (!expectedSha256) {
    throw new Error("missing expected sha256");
  }

  const res = await request(url, githubHeaders({ Accept: "application/octet-stream" }), 5);
  await fs.promises.mkdir(path.dirname(outPath), { recursive: true });

  const tmpPath = `${outPath}.tmp`;
  const hash = crypto.createHash("sha256");
  const ws = fs.createWriteStream(tmpPath, { mode: 0o755 });

  await new Promise((resolve, reject) => {
    res.on("data", (chunk) => hash.update(chunk));
    res.pipe(ws);
    res.on("error", reject);
    ws.on("error", reject);
    ws.on("finish", resolve);
  });

  const got = hash.digest("hex").toLowerCase();
  if (got !== expectedSha256) {
    await fs.promises.rm(tmpPath, { force: true });
    throw new Error(`sha256 mismatch for ${path.basename(outPath)}: expected ${expectedSha256}, got ${got}`);
  }

  await fs.promises.rename(tmpPath, outPath);
}

async function main() {
  const goos = goosFromPlatform(process.platform);
  const goarch = goarchFromArch(process.arch);
  if (!goos) fail(`unsupported platform: ${process.platform}`);
  if (!goarch) fail(`unsupported architecture: ${process.arch}`);

  const pkg = require(path.join(__dirname, "..", "package.json"));
  if (!pkg.version || typeof pkg.version !== "string") {
    fail("missing package version");
  }

  const repo = process.env.CEFAS_GITHUB_REPO || "CefasDB/cefasdb-core";
  const tag = process.env.CEFAS_RELEASE_TAG || `v${pkg.version}`;
  const ext = goos === "windows" ? ".exe" : "";
  const assetName = `cefas-${goos}-${goarch}${ext}`;
  const base = `https://github.com/${repo}/releases/download/${tag}`;

  log(`downloading ${assetName} (${tag})`);

  const sums = await downloadText(`${base}/SHA256SUMS.txt`);
  const expected = parseExpectedSha256(sums, assetName);
  if (!expected) {
    fail(`checksum entry not found for ${assetName} in SHA256SUMS.txt`);
  }

  const outPath = path.join(__dirname, "..", "native", `cefas${ext}`);
  await downloadFileWithSha256(`${base}/${assetName}`, outPath, expected);

  if (goos !== "windows") {
    await fs.promises.chmod(outPath, 0o755);
  }

  log(`installed ${path.relative(process.cwd(), outPath)}`);
}

main().catch((err) => fail(err && err.message ? err.message : String(err)));
