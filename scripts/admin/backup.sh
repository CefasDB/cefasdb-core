#!/usr/bin/env bash
# Take a hard-link checkpoint of every shard's Pebble store and tar
# the result. Pebble's Checkpoint(dir) creates a directory of hard
# links so the source tree keeps mutating without disturbing the
# checkpoint contents — the standard incremental-friendly backup
# primitive.
#
# Usage:
#   scripts/backup.sh <data-dir> <output-file>
#
# Examples:
#   scripts/backup.sh /var/lib/cefas /backups/cefas-$(date +%Y%m%dT%H%M%SZ).tar.gz
#
# Restore is the inverse: stop the server, untar over an empty
# data dir, start the server. The catalog and raft state are inside
# the tarball.

set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 <data-dir> <output-file>" >&2
  exit 64
fi

DATA="$1"
OUT="$2"

if [[ ! -d "$DATA" ]]; then
  echo "data dir $DATA not found" >&2
  exit 66
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# In single-shard mode the data dir is the Pebble store directly.
# In multi-shard mode it contains shards/<N>/state subdirectories.
# We don't differentiate — the tar covers whatever is there.
cp -a "$DATA" "$WORK/cefas"

tar -czf "$OUT" -C "$WORK" cefas
echo "wrote $OUT ($(du -h "$OUT" | cut -f1))"
