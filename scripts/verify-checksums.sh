#!/usr/bin/env sh
set -eu
cd "$(dirname "$0")/.."
if command -v sha256sum >/dev/null 2>&1; then
  sha256sum --check evidence/checksums.sha256
elif command -v shasum >/dev/null 2>&1; then
  shasum -a 256 --check evidence/checksums.sha256
else
  echo "sha256sum or shasum is required" >&2
  exit 2
fi
