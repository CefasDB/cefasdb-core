# @cefasdb/cefas

This package installs the `cefas` CLI as a prebuilt Go binary downloaded from
GitHub Releases during `npm install`.

## Install

```sh
npm install -g @cefasdb/cefas
cefas --help
```

Node.js 18 or newer is required.

## Upgrade

```sh
npm install -g @cefasdb/cefas@latest
```

## Environment Overrides

- `CEFAS_GITHUB_REPO` changes the GitHub repository used for downloads. The
  default is `CefasDB/cefasdb-core`.
- `CEFAS_RELEASE_TAG` downloads a specific release tag instead of `v<version>`.
- `CEFAS_GITHUB_TOKEN`, `GITHUB_TOKEN`, or `GH_TOKEN` can authenticate release
  downloads for private repositories or higher rate limits.
