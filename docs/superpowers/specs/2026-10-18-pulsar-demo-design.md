# Apache Pulsar Event-Driven Demo — Design

**Date:** 2026-10-18
**Goal:** A local, screenshot-friendly demo showing how microservices communicate through events using Apache Pulsar: a producer publishes an event, Pulsar stores it in a topic, and subscribed consumers receive it. Pulsar Express is used as the GUI to inspect the cluster.

## Requirements (from the user)

- Standalone Pulsar running in Docker
- Pulsar Express GUI to inspect the cluster
- Producer and consumers in Node.js
- Domain: orders
- Clear console logs, a small web page to send events, a Pulsar admin dashboard, multiple consumers

## Assumptions

- Runs only on a local machine (Windows + Docker Desktop); no authentication or TLS
- One command starts everything: `docker compose up --build`

## Concepts the demo illustrates

| Concept | Meaning | Where it appears in the demo |
|---|---|---|
| **Producer** | A service that publishes events | `order-service` |
| **Broker** | Middleware that receives, stores and delivers events | `pulsar` container |
| **Topic** | A named channel that events are published to | `persistent://public/default/orders` (tenant `public`, namespace `default`, topic `orders`) |
| **Subscription** | A named cursor on a topic. Each subscription gets its own copy of every message | `inventory` and `notifications` |
| **Consumer** | A service attached to a subscription | the inventory and notification services |
| **Ack** (acknowledgement) | The consumer confirms it processed a message, so Pulsar won't redeliver it | each consumer acks after logging |
| **Backlog** | Messages not yet acked by a subscription; Pulsar keeps them until they are | visible with `pulsar-admin topics stats` |
| **Exclusive subscription** | Only one consumer may attach to the subscription | `notifications` |
| **Shared subscription** | Several consumers attach to the subscription, and messages are spread round-robin between them | `inventory` (2 consumers) |

Key takeaways:
1. **Fan-out.** Two different subscriptions each receive every order.
2. **Load balancing.** Within the Shared subscription, the two inventory instances split the orders.
3. **Decoupling.** The producer knows only the topic, never the consumers.
4. **Durability.** If a consumer is stopped, its messages wait in the backlog and arrive when it restarts.

## Architecture

All services are defined in a single `docker-compose.yml`:

| Container | Image / build | Host port | Role |
|---|---|---|---|
| `pulsar` | `apachepulsar/pulsar` (4.0 LTS), command `bin/pulsar standalone` | 6650 (binary protocol), 8080 (admin REST) | Broker, with BookKeeper storage and metadata in a single process |
| `pulsar-express` | `bbonnin/pulsar-express`, `PE_CONNECTION_URL=http://pulsar:8080` | 3001 (container port 3000; host 3000 was already in use) | GUI for clusters, tenants, namespaces and topics |
| `order-service` | `./producer` (Node.js + Express) | 4000 | Web form plus `POST /orders`; publishes order events |
| `inventory-service-1` | `./consumer` | – | Subscription `inventory`, type Shared |
| `inventory-service-2` | `./consumer` | – | Subscription `inventory`, type Shared |
| `notification-service` | `./consumer` | – | Subscription `notifications`, type Exclusive |

- `pulsar` has a healthcheck (`bin/pulsar-admin brokers healthcheck`). Every other container uses `depends_on: condition: service_healthy`.
- The three consumers share one image and are configured by environment variables: `SERVICE_NAME`, `SUBSCRIPTION`, `SUBSCRIPTION_TYPE`, `PULSAR_URL`, `TOPIC`.

## Data flow

```
Browser form ──HTTP POST /orders──▶ order-service ──publish──▶ [ Pulsar topic: orders ]
                                                                   │
                                   ┌───────────────────────────────┴──────────────────────┐
                         subscription "inventory" (Shared)              subscription "notifications" (Exclusive)
                          ┌───────────┴───────────┐                                │
                 inventory-service-1    inventory-service-2                notification-service
                 (gets ~half)           (gets ~half)                       (gets every order)
```

### Event schema (JSON)

```json
{ "orderId": "uuid", "product": "Laptop", "quantity": 2, "createdAt": "ISO-8601 timestamp" }
```

The message key is `orderId`, and the message property `eventType=OrderCreated` is set on every event.

## Components

### order-service (`producer/`)
- `GET /` serves `public/index.html`: a form (product, quantity), a "Send 10 random orders" button, and a list of sent orders with their message IDs
- `POST /orders` takes a JSON body `{product, quantity}`, publishes the event and returns `{orderId, messageId}`
- `POST /orders/random?count=N` publishes N random orders
- Logs a colored line for every send: `📤 SENT order <id> product=… qty=… → orders msgId=…`

### consumer (`consumer/`)
- Subscribes using the environment variable configuration and receives in a loop
- Logs a colored line for every message: `📥 [<service> | sub=<name> <type>] RECEIVED order <id> product=… qty=… msgId=…`, followed by `✅ ACK`
- Each service name gets its own colour, so screenshots are easy to read

## Error handling

- Before Pulsar is healthy, the compose `depends_on` health condition stops the services from starting
- If publishing fails, `POST /orders` returns 500 with the error message, and the web page shows it
- If a consumer receives invalid JSON, it logs a warning and acks the message, so the demo never gets stuck in a redelivery loop
- On SIGINT/SIGTERM, consumers and the producer close their Pulsar client cleanly

## Verification

1. `docker compose up --build`: all containers start and the consumers log that they are subscribed
2. `curl -X POST localhost:4000/orders/random?count=10` sends 10 orders:
   - `notification-service` logs 10 received orders
   - the two inventory services together log 10, split between them
3. `docker compose exec pulsar bin/pulsar-admin topics stats persistent://public/default/orders` shows both subscriptions with `msgBacklog: 0`
4. Durability check: run `docker compose stop notification-service`, send 5 orders, and confirm a backlog of 5. Then run `docker compose start notification-service` and confirm all 5 are delivered
5. Pulsar Express at `localhost:3001` shows the cluster, the `public/default` namespace and the `orders` topic

## Deliverable: README.md

The README covers:
- What the demo shows, using the concept table above
- How to run and stop it
- A step-by-step demo script, with a list of the screenshots to take at each step

## Out of scope

Multi-node clusters, authentication, schema registry, dead-letter queues, Pulsar Functions.
