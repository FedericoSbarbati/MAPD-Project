#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GIULIA_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_DIR="${MAPD_GIULIA_ENV_DIR:-$GIULIA_DIR/.venv}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

cd "$GIULIA_DIR"

if [ ! -d "$ENV_DIR" ]; then
  "$PYTHON_BIN" -m venv "$ENV_DIR"
fi

"$ENV_DIR/bin/python" -m pip install --upgrade pip wheel
"$ENV_DIR/bin/python" -m pip install -r requirements.txt
"$ENV_DIR/bin/python" -m ipykernel install \
  --user \
  --name mapd-covid-giulia \
  --display-name "Python (mapd-covid-giulia)"

cat <<EOF
Ambiente Giulia pronto:
  source "$ENV_DIR/bin/activate"

Kernel Jupyter:
  Python (mapd-covid-giulia)
EOF
