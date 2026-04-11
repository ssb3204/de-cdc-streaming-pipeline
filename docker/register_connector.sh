#!/usr/bin/env bash
# Debezium 커넥터 등록 스크립트
# .env (또는 환경변수)에서 DBZ_USER / DBZ_PASSWORD를 읽어 템플릿에 주입 후 Kafka Connect REST API에 등록.
#
# 사용법:
#   cd docker
#   source ../.env && bash register_connector.sh
#
# 또는 환경변수를 직접 지정:
#   DBZ_USER=debezium DBZ_PASSWORD=secret bash register_connector.sh

set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
TEMPLATE_FILE="$(dirname "$0")/register-mysql-debezium.json.template"

if [[ -z "${DBZ_USER:-}" || -z "${DBZ_PASSWORD:-}" ]]; then
  echo "[ERROR] DBZ_USER and DBZ_PASSWORD must be set (source .env or export manually)"
  exit 1
fi

echo "[info] Registering Debezium connector at ${CONNECT_URL} ..."

envsubst < "$TEMPLATE_FILE" | curl -s -o /dev/null -w "%{http_code}" \
  -X POST "${CONNECT_URL}/connectors" \
  -H "Content-Type: application/json" \
  -d @- | tee /dev/stderr | grep -q "^201$" \
  && echo "[OK] Connector registered successfully" \
  || echo "[WARN] Unexpected response (connector may already exist — check above)"
