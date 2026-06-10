#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");
const { spawnSync } = require("child_process");

const exe = process.platform === "win32" ? ".exe" : "";
const binPath = path.join(__dirname, "..", "native", `cefas${exe}`);

try {
  fs.accessSync(binPath, fs.constants.X_OK);
} catch {
  console.error("cefas: native binary is missing. Try reinstalling the package.");
  process.exit(1);
}

const res = spawnSync(binPath, process.argv.slice(2), { stdio: "inherit" });
if (res.error) {
  console.error(`cefas: failed to run native binary: ${res.error.message}`);
  process.exit(1);
}

process.exit(typeof res.status === "number" ? res.status : 1);
