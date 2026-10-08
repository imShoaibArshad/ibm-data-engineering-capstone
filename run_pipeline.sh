#!/usr/bin/env bash
#
# Run the full data pipeline: extract -> stage -> quality -> load -> marts -> report.
#
#   ./run_pipeline.sh                     # everything, from scratch
#   ./run_pipeline.sh --dry-run           # print the task DAG, execute nothing
#   ./run_pipeline.sh --start load.warehouse   # resume from a task
#   ./run_pipeline.sh --stop quality.staging   # run only the first half
#
# Exit codes: 0 = success, 1 = a quality gate failed, 2 = the run errored.
set -euo pipefail
cd "$(dirname "$0")"

# Prefer the project virtualenv, then any python on PATH.
if [[ -x .venv/bin/python ]]; then
  PY=.venv/bin/python
elif [[ -x .venv/Scripts/python.exe ]]; then
  PY=.venv/Scripts/python.exe        # Git Bash / MSYS on Windows
else
  PY=${PYTHON:-python3}
fi

export PYTHONPATH="${PYTHONPATH:+$PYTHONPATH:}src"
exec "$PY" -m retailde run "$@"
