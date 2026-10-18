# Apache Pulsar Event-Driven Demo Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Docker Compose demo with standalone Pulsar, the Pulsar Express GUI, a Node.js/Express order producer and three Node.js consumers on two subscriptions.

**Architecture:** One `docker-compose.yml` starts `pulsar` (standalone, with a healthcheck), `pulsar-express`, `order-service` (built from `producer/`), and three consumer containers built from the single image in `consumer/`, each configured through environment variables. Events are JSON `OrderCreated` messages on `persistent://public/default/orders`.

**Tech Stack:** `apachepulsar/pulsar:4.0.14`, `bbonnin/pulsar-express`, Node 22 (`node:22-bookworm-slim`, which uses glibc as the `pulsar-client` native binary requires), `pulsar-client@^1.18.0`, `express@^5.1.0`. Colours come from plain ANSI codes, so there is no chalk dependency.

**Spec:** `docs/superpowers/specs/2026-10-18-pulsar-demo-design.md`

## Global Constraints

- Topic: `persistent://public/default/orders`
- Subscriptions: `inventory` (Shared, 2 consumers) and `notifications` (Exclusive, 1 consumer)
- Event: `{ orderId, product, quantity, createdAt }`, with key = `orderId` and property `eventType=OrderCreated`
- Ports: 6650 and 8080 (Pulsar), 3001 (Pulsar Express on the host), 4000 (order-service)
- Producer log format: `📤 SENT order <id> product=… qty=… → orders msgId=…`
- Consumer log format: `📥 [<service> | sub=<name> <type>] RECEIVED order <id> product=… qty=… msgId=…`, then `✅ ACK`

## Review Focus

1. Services start before Pulsar is ready → the Pulsar healthcheck plus `depends_on: service_healthy` stops this; the client also retries the connection.
2. Invalid quantity or missing product on `POST /orders` → return 400 with a message and publish nothing.
3. A non-JSON message on the topic → the consumer logs a warning and acks it, so there is no redelivery loop.
4. A second consumer attaching to the Exclusive subscription → not done in the demo; the README explains why it would fail.
5. Pulsar Express cannot reach Pulsar → it must use the in-network URL `http://pulsar:8080`, not localhost.

---

### Task 1: Pulsar and Pulsar Express in Compose

**Files:** Create `docker-compose.yml` and `.gitignore`.

- [ ] Define the `pulsar` service: image `apachepulsar/pulsar:4.0.14`, command `bin/pulsar standalone`, ports `6650:6650` and `8080:8080`, healthcheck `bin/pulsar-admin brokers healthcheck` (interval 10s, retries 20).
- [ ] Define the `pulsar-express` service: port `3001:3000`, `PE_CONNECTION_URL=http://pulsar:8080`, `PE_CONNECTION_NAME=pulsar-demo`, depends on `pulsar` being healthy.
- [ ] Verify: `docker compose up -d pulsar pulsar-express` shows pulsar as `healthy`, and `curl localhost:8080/admin/v2/clusters` returns `["standalone"]`.

### Task 2: Order producer (order-service)

**Files:** Create `producer/package.json`, `producer/server.js`, `producer/public/index.html`, `producer/Dockerfile` and `producer/.dockerignore`.

**Interfaces:** `POST /orders` with body `{product: string, quantity: int>0}` returns `201 {orderId, messageId, event}`. Invalid input returns `400 {error}`. `POST /orders/random?count=N` (N from 1 to 100, default 10) returns `201 {sent: [...]}`.

- [ ] `server.js`: create a Pulsar client from `PULSAR_URL` (default `pulsar://localhost:6650`) and a producer on `TOPIC`. Publish with `producer.send({data: Buffer, partitionKey: orderId, properties: {eventType: 'OrderCreated'}})`. Log the send in the global format. Serve `public/` statically. Close cleanly on SIGINT/SIGTERM.
- [ ] `index.html`: an order form, a "Send 10 random orders" button, and a table of sent orders with their msgIds. Show errors inline.
- [ ] Add the `order-service` service to compose: build `./producer`, port `4000:4000`, depends on `pulsar` being healthy.
- [ ] Verify: `curl -X POST localhost:4000/orders -H 'content-type: application/json' -d '{"product":"Laptop","quantity":2}'` returns 201 with a messageId; `{"quantity":0}` returns 400.

### Task 3: Consumers

**Files:** Create `consumer/package.json`, `consumer/consumer.js`, `consumer/Dockerfile` and `consumer/.dockerignore`.

**Interfaces:** The environment variables are `SERVICE_NAME`, `SUBSCRIPTION`, `SUBSCRIPTION_TYPE` (`Exclusive` or `Shared`), `PULSAR_URL` and `TOPIC`.

- [ ] `consumer.js`: subscribe with `subscriptionInitialPosition: 'Earliest'`, then loop on `receive()`. Parse JSON, log in the global format with a colour per service, then `acknowledge`. On a parse error, log a warning and ack. Close cleanly on SIGINT/SIGTERM.
- [ ] Add `inventory-service-1` and `inventory-service-2` (inventory, Shared) and `notification-service` (notifications, Exclusive) to compose.
- [ ] Verify: after `POST /orders/random?count=10`, notification-service logs 10 orders, and the two inventory services log 10 in total, split between them.

### Task 4: End-to-end verification and README

**Files:** Create `README.md`.

- [ ] Run the spec's verification steps 1–5, including the durability check (stop notification-service, send 5 orders, see a backlog of 5, start it again, see all 5 delivered).
- [ ] Check Pulsar Express at `localhost:3001` shows the topic. If its topic view is too limited, record that in the README.
- [ ] README: what the demo shows (concept table), how to run and stop it, the demo script, the screenshot checklist, and troubleshooting.
