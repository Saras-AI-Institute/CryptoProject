#!/usr/bin/env bash
set -euo pipefail

# Loads environment variables from an optional .env file when present.
if [[ -f ".env" ]]; then
  # shellcheck disable=SC1090
  source ".env"
fi

if [[ -z "${DB_PASSWORD:-}" ]]; then
  echo "DB_PASSWORD must be exported or present in the .env file" >&2
  exit 1
fi

python src/ingest.py
python src/load_data.py
