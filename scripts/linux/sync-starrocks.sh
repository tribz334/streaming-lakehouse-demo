#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

# The synchronization contains extensive typed Tableau-output parsing. PowerShell
# Core is cross-platform, so the Bash entry reuses the verified implementation
# until the parser is replaced by a language-neutral export service.
if ! command -v pwsh >/dev/null 2>&1; then
  echo "sync-starrocks.sh requires cross-platform PowerShell Core (pwsh)." >&2
  echo "Install PowerShell 7, then rerun this Bash entry." >&2
  exit 1
fi

pwsh -NoLogo -NoProfile -File "$ROOT/scripts/windows/sync-starrocks-olap.ps1"
