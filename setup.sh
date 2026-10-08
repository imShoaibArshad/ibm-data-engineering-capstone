#!/usr/bin/env bash
#
# Create the virtual environment and install every dependency.
# Idempotent -- safe to re-run. No cloud account, no Docker required.
#
#   ./setup.sh          # runtime dependencies
#   ./setup.sh --dev    # runtime + pytest
set -euo pipefail
cd "$(dirname "$0")"

if [[ -x .venv/bin/python ]]; then
  PY=.venv/bin/python
  echo "Reusing existing .venv"
else
  echo "Creating virtual environment..."
  "${PYTHON:-python3}" -m venv .venv
  PY=.venv/bin/python
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
"$PY" -m pip list --format=freeze |
  grep -E '^(duckdb|pandas|pyarrow|networkx|click|matplotlib|pytest)=' |
  sed 's/^/  /'

cat <<'EOF'

Next steps:
  ./run_pipeline.sh          # run the whole pipeline
  ./run_pipeline.sh --dry-run  # inspect the task DAG first
  .venv/bin/python -m pytest tests   # run the test suite
EOF
