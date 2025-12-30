#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

TMP_DIR="$(mktemp -d 2>/dev/null || mktemp -d -t ferrite-evidence)"
DB_PATH="${TMP_DIR}/ferrite-evidence.db"
LOG_PATH="${TMP_DIR}/ferrite-evidence.log"

echo "[evidence] temp dir: ${TMP_DIR}"
echo "[evidence] db: ${DB_PATH}"
echo "[evidence] log: ${LOG_PATH}"

(
  cd "${ROOT_DIR}"
  cargo run --quiet --example crash_recovery_evidence -- setup "${DB_PATH}" "${LOG_PATH}"
)

set +e
(
  cd "${ROOT_DIR}"
  cargo run --quiet --example crash_recovery_evidence -- crash "${DB_PATH}" "${LOG_PATH}"
)
CRASH_EXIT_CODE=$?
set -e

if [[ "${CRASH_EXIT_CODE}" -eq 0 ]]; then
  echo "[evidence] ERROR: crash step unexpectedly returned exit code 0"
  exit 1
fi

echo "[evidence] crash step exited non-zero as expected (${CRASH_EXIT_CODE})"

(
  cd "${ROOT_DIR}"
  cargo run --quiet --example crash_recovery_evidence -- verify "${DB_PATH}" "${LOG_PATH}"
)

echo "[evidence] success"
