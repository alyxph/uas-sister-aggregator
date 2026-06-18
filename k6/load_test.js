/**
 * k6/load_test.js — K6 load testing script for Pub-Sub Log Aggregator.
 *
 * Simulates concurrent publishers sending events to POST /publish.
 * Tests throughput, latency, and dedup correctness under load.
 *
 * Run:
 *   k6 run k6/load_test.js
 *
 * Requirements:
 *   - Aggregator running at http://localhost:8080
 *   - Install K6: https://k6.io/docs/get-started/installation/
 *
 * GitHub reference for K6 usage:
 *   https://github.com/grafana/k6
 */

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import { randomString } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

// Custom metrics
const eventsPublished = new Counter('events_published');
const duplicatesSent = new Counter('duplicates_sent');
const publishLatency = new Trend('publish_latency', true);

// Test configuration
export const options = {
  stages: [
    { duration: '10s', target: 20 },   // ramp up to 20 VUs
    { duration: '30s', target: 50 },   // ramp up to 50 VUs
    { duration: '1m',  target: 50 },   // sustained load
    { duration: '10s', target: 0 },    // ramp down
  ],
  thresholds: {
    http_req_duration: ['p(95)<1000'],  // 95% requests under 1s
    http_req_failed: ['rate<0.05'],     // less than 5% errors
    events_published: ['count>5000'],   // at least 5000 events published
  },
};

const BASE_URL = __ENV.AGGREGATOR_URL || 'http://localhost:8080';
const TOPICS = ['topic.A', 'topic.B', 'topic.C', 'topic.D', 'topic.E'];
const DUP_RATE = 0.35;  // 35% duplication rate

// Shared pool of event IDs for creating duplicates
const sharedEventIds = [];
for (let i = 0; i < 100; i++) {
  sharedEventIds.push(`shared-${randomString(12)}`);
}

function makeEvent(eventId, topic) {
  return {
    topic: topic || TOPICS[Math.floor(Math.random() * TOPICS.length)],
    event_id: eventId,
    timestamp: new Date().toISOString(),
    source: `k6-vu-${__VU}`,
    payload: {
      value: Math.floor(Math.random() * 9999),
      iteration: __ITER,
      vu: __VU,
    },
  };
}

export default function () {
  const isDuplicate = Math.random() < DUP_RATE;

  let event;
  if (isDuplicate) {
    // Pick a shared event_id to simulate duplicate
    const sharedId = sharedEventIds[Math.floor(Math.random() * sharedEventIds.length)];
    event = makeEvent(sharedId);
    duplicatesSent.add(1);
  } else {
    // Unique event
    const uniqueId = `k6-${__VU}-${__ITER}-${randomString(8)}`;
    event = makeEvent(uniqueId);
  }

  const res = http.post(
    `${BASE_URL}/publish`,
    JSON.stringify(event),
    { headers: { 'Content-Type': 'application/json' } }
  );

  check(res, {
    'status is 202': (r) => r.status === 202,
    'response has queued field': (r) => JSON.parse(r.body).queued !== undefined,
  });

  publishLatency.add(res.timings.duration);
  eventsPublished.add(1);

  sleep(0.05);  // 50ms between requests per VU
}

// After test, check stats
export function teardown() {
  const statsRes = http.get(`${BASE_URL}/stats`);
  console.log('=== Aggregator Stats After Load Test ===');
  console.log(statsRes.body);

  const stats = JSON.parse(statsRes.body);
  check(stats, {
    'has received count': (s) => s.received > 0,
    'has unique_processed': (s) => s.unique_processed > 0,
    'has duplicate_dropped': (s) => s.duplicate_dropped > 0,
    'dedup working (unique < received)': (s) => s.unique_processed < s.received,
  });
}
