#!/usr/bin/env bash
#
# Create the virtual environment and install every dependency.
# Idempotent -- safe to re-run. No cloud account, no Docker required.
#
#   ./setup.sh          # runtime dependencies
#   ./setup.sh --dev    # runtime + pytest
set -euo pipefail
cd "$(dirname "$0")"

# Same detection as run_pipeline.sh, so the two work as a pair on
# Linux/macOS (bin/) and on Git Bash / MSYS (Scripts/).
if [[ -x .venv/bin/python ]]; then
  PY=.venv/bin/python
  echo "Reusing existing .venv"
elif [[ -x .venv/Scripts/python.exe ]]; then
  PY=.venv/Scripts/python.exe
  echo "Reusing existing .venv (Windows layout)"
else
  echo "Creating virtual environment..."
  "${PYTHON:-python3}" -m venv .venv
  if [[ -x .venv/bin/python ]]; then
    PY=.venv/bin/python
  else
    PY=.venv/Scripts/python.exe
  fi
fi

"$PY" -m pip install --upgrade pip --quiet
"$PY" -m pip install -r requirements.txt --quiet
# Editable install registers the `retailde` console command and makes
# `python -m retailde` work from any directory -- no PYTHONPATH juggling.
"$PY" -m pip install -e . --quiet
if [[ "${1:-}" == "--dev" ]]; then
  "$PY" -m pip install pytest --quiet
fi

echo
echo "Installed:"
# `|| true` because pipefail + a grep that matches nothing would otherwise
# abort the script under `set -e` right before the usage hints.
"$PY" -m pip list --format=freeze |
  { grep -E '^(duckdb|pandas|pyarrow|networkx|click|matplotlib|pytest)=' || true; } |
  sed 's/^/  /'

cat <<'EOF'

Next steps:
  source .venv/bin/activate     # or .venv\Scripts\Activate.ps1 on Windows
  ./run_pipeline.sh             # run the whole pipeline
  ./run_pipeline.sh --dry-run   # inspect the task DAG first
  python -m pytest tests        # run the test suite
EOF
