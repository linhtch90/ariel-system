#!/usr/bin/env bash
set -euo pipefail

GATEWAY_URL="${GATEWAY_URL:-http://localhost:8080}"
TENANT_ID="${TENANT_ID:-tenant-acme}"
IDEM_KEY="idem-bench-$(date +%s)"

echo "=== Project Ariel Fast-Path & Idempotency Smoke/Load Verification ==="
echo "Target Gateway: ${GATEWAY_URL}"
echo "Idempotency-Key: ${IDEM_KEY}"

PAYLOAD='{
  "accountId": "acc-benchmark-001",
  "counterpartyId": "merchant-global-99",
  "amount": 250.75,
  "currency": "USD",
  "region": "US-EAST",
  "deviceFingerprint": "fp-trusted-ios-19",
  "metadata": {
    "channel": "mobile-checkout"
  }
}'

echo "1. Sending initial transaction request..."
curl -i -s -X POST "${GATEWAY_URL}/api/v1/transactions" \
  -H "Content-Type: application/json" \
  -H "X-Tenant-Id: ${TENANT_ID}" \
  -H "Idempotency-Key: ${IDEM_KEY}" \
  -d "${PAYLOAD}"
echo -e "\n"

echo "2. Sending duplicate retry with identical Idempotency-Key (expecting HTTP 200 idempotentReplay=true)..."
curl -i -s -X POST "${GATEWAY_URL}/api/v1/transactions" \
  -H "Content-Type: application/json" \
  -H "X-Tenant-Id: ${TENANT_ID}" \
  -H "Idempotency-Key: ${IDEM_KEY}" \
  -d "${PAYLOAD}"
echo -e "\n"

echo "3. Querying CQRS real-time tenant analytics summary..."
curl -i -s "${GATEWAY_URL}/api/v1/analytics/tenants/${TENANT_ID}/summary" \
  -H "X-Tenant-Id: ${TENANT_ID}"
echo -e "\n"
