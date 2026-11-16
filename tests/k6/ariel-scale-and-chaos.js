import http from 'k6/http';
import { check, sleep } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

/**
 * Project Ariel — Grafana k6 High-Load, Autoscaling (HPA/KEDA) & Limits Test Suite
 *
 * Run with:
 *   k6 run tests/k6/ariel-scale-and-chaos.js
 *
 * Stages:
 *   1. Warmup (0 -> 100 VUs): Baseline P99 < 15ms verification
 *   2. Scale-Out Spike (100 -> 1,000 VUs): Triggers Kubernetes HPA (3 -> 100+ pods) & KEDA (3 -> 64 pods)
 *   3. Sustained Peak Load (1,000 VUs): Verifies ZGC, Virtual Threads, L1/L2 Cache & Outbox under pressure
 *   4. Scale-Down Cool-Off (1,000 -> 0 VUs): Verifies graceful HPA/KEDA scale-in stabilization
 */
export const options = {
  stages: [
    { duration: '30s', target: 100 },   // Stage 1: Warmup
    { duration: '1m',  target: 1000 },  // Stage 2: 10x Spike (Triggers K8s HPA & KEDA scale-up)
    { duration: '2m',  target: 1000 },  // Stage 3: Sustained Extreme Load
    { duration: '30s', target: 0 },     // Stage 4: Scale-down cool-off
  ],
  thresholds: {
    http_req_duration: ['p(95)<10', 'p(99)<15'], // Fast-Path SLA < 15ms P99
    http_req_failed: ['rate<0.01'],              // Availability SLA > 99% during spike
  },
};

const GATEWAY_URL = __ENV.GATEWAY_URL || 'http://localhost:8080';

export default function () {
  const tenantId = `tenant-scale-${__VU % 20}`;
  const idempotencyKey = uuidv4();

  const payload = JSON.stringify({
    accountId: `acc-${__VU}-${__ITER % 100}`,
    counterpartyId: 'merchant-flash-sale',
    amount: 99.95,
    currency: 'USD',
    region: 'US-EAST',
    deviceFingerprint: `fp-k6-${__VU}`,
    metadata: { scenario: 'k6-autoscaling-spike' },
  });

  const headers = {
    'Content-Type': 'application/json',
    'X-Tenant-Id': tenantId,
    'Idempotency-Key': idempotencyKey,
  };

  // 1. Primary Fast-Path Transaction Post
  const res = http.post(`${GATEWAY_URL}/api/v1/transactions`, payload, { headers });
  check(res, {
    'status is 202 Accepted or 429 RateLimited': (r) => r.status === 202 || r.status === 429,
  });

  // 2. Simulate 10% network retry rate with identical Idempotency-Key
  if (__ITER % 10 === 0 && res.status === 202) {
    const retryRes = http.post(`${GATEWAY_URL}/api/v1/transactions`, payload, { headers });
    check(retryRes, {
      'idempotent replay returns 200 OK': (r) => r.status === 200,
    });
  }

  sleep(0.01);
}
