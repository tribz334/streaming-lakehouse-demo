#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

if ! command -v pwsh >/dev/null 2>&1; then
  echo "run-paimon-experiments.sh requires cross-platform PowerShell Core (pwsh)." >&2
  exit 1
fi

pwsh -NoLogo -NoProfile -File "$ROOT/scripts/windows/run-paimon-experiments.ps1"
