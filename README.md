# Apache Pulsar — Event-Driven Microservices Demo

A small, local demo of how microservices communicate through **events** using **Apache Pulsar**:

- **order-service** (Node.js + Express) **publishes** `OrderCreated` events
- **Apache Pulsar** (standalone, in Docker) stores them in the topic `orders`
- **inventory-service ×2**, **notification-service** and **billing-service** **subscribe** and react
- **Pulsar Express** is a web GUI for inspecting the cluster

```
Browser ──POST /orders──▶ order-service ──publish──▶ [ Pulsar topic: orders ]
                                                          │
                ┌─────────────────────────────────────────┼──────────────────────────────────┐
   subscription "inventory" (Shared)        subscription "notifications" (Exclusive)   subscription "billing" (Exclusive)
      ┌──────────┴──────────┐                             │                                  │
inventory-service-1  inventory-service-2         notification-service                  billing-service
 (~half the orders)   (~half the orders)            (every order)                       (every order)
```

## Why events instead of direct HTTP calls?

With direct calls, the order service would need to know about the inventory, notification and billing services. It would call each one, and fail if either were down. With events, it **announces a fact** ("an order was created") to a broker and moves on:

| Benefit | What you see in this demo |
|---|---|
| **Decoupling** | `order-service` only knows the topic name. It has no idea who consumes the events. |
| **Fan-out** | Each subscription (`inventory`, `notifications`, `billing`) gets its **own copy of every event**. |
| **Load balancing / scaling** | The two inventory instances share a **Shared** subscription, so Pulsar splits the orders between them. |
| **Durability / resilience** | Stop a consumer and its events wait in the **backlog**. When it restarts, it receives every one. |

## Pulsar concepts in 1 minute

| Concept | Meaning |
|---|---|
| **Producer** | Publishes messages to a topic (`order-service`). |
| **Topic** | A named channel: `persistent://public/default/orders` (the parts are tenant / namespace / topic). |
| **Subscription** | A named cursor on a topic. Each subscription receives every message. |
| **Subscription type** | **Exclusive**: only one consumer may attach. **Shared**: many consumers attach, and messages are spread round-robin between them. (Pulsar also has Failover and Key_Shared.) |
| **Consumer** | A process attached to a subscription. |
| **Ack** | The consumer confirms it processed a message, and Pulsar won't redeliver it. |
| **Backlog** | Messages a subscription has not acknowledged yet. |
| **Message ID** `(5,3,-1,-1)` | Where the message is stored: (ledger, entry, partition, batch index). A **ledger** is a BookKeeper storage segment. |

## Run it

Requirements: Docker Desktop (about 2 GB of free RAM).

```bash
docker compose up -d --build     # first run downloads ~1 GB of images
docker compose ps                # wait until pulsar shows "(healthy)", about 30-60 s
```

| What | URL |
|---|---|
| Order Service web page (producer) | http://localhost:4000 |
| Pulsar Express (cluster GUI) | http://localhost:3001 |
| Pulsar admin REST API | http://localhost:8080/admin/v2/clusters |

Watch the services talk (keep this running in its own terminal):

```bash
docker compose logs -f order-service inventory-service-1 inventory-service-2 notification-service billing-service
```

Stop everything with `docker compose down`. To also delete the stored messages, run `docker compose down -v`.

## Demo script and screenshots

| # | Step | Screenshot |
|---|---|---|
| 1 | `docker compose ps` | All 7 containers up, with `pulsar` **healthy** |
| 2 | Open the logs (command above) | 4 consumers print `✔ Subscribed`, and the producer prints `✔ Connected` |
| 3 | Open http://localhost:3001 → Clusters / Tenants / Namespaces / Topics | Pulsar Express showing the `standalone` cluster and the `public/default/orders` topic |
| 4 | On http://localhost:4000, send **one** order | The web page with the message ID, plus logs: 1 `📤 SENT`, then 3 `📥 RECEIVED`: in **notification-service, billing-service, and exactly one** inventory service (**fan-out**) |
| 5 | Click **Send 10 random orders** | Logs: notification-service and billing-service **each** receive all 10, while inventory-service-1 and -2 **alternate** (**load balancing**) |
| 6 | Topic stats (Pulsar Express topic page, or the command below) | 3 subscriptions, all with `msgBacklog: 0`, and `inventory` showing 2 consumers |
| 7 | `docker compose stop notification-service`, then send 5 orders | Stats: `notifications` **backlog = 5**, with no consumers, while inventory and billing still process everything |
| 8 | `docker compose start notification-service` | Logs: it immediately receives the **5 missed orders, in order**, and the backlog goes back to 0 (**durability**) |

Topic stats from the command line:

```bash
docker compose exec pulsar bin/pulsar-admin topics stats persistent://public/default/orders
# or:  curl localhost:8080/admin/v2/persistent/public/default/orders/stats
```

Send orders without the browser:

```bash
curl -X POST localhost:4000/orders -H "content-type: application/json" -d '{"product":"Laptop","quantity":2}'
curl -X POST "localhost:4000/orders/random?count=10"
```

### Things to try

- **Add a new service without touching the producer.** Copy the `billing-service` block in `docker-compose.yml`, rename it to `analytics-service` with `SUBSCRIPTION: analytics`, and run `docker compose up -d`. Because a new subscription starts at `Earliest`, it first catches up on **every order ever published**, then keeps receiving new ones (fan-out to a 4th subscription).
- **Break the Exclusive rule.** Run `docker compose up -d --scale notification-service=2`. The second instance fails with *"Exclusive consumer is already connected"* and keeps restarting. This is why Shared exists.

## Project layout

```
docker-compose.yml     Pulsar, Pulsar Express, producer and 4 consumers
producer/              order-service: Express API + web page (public/index.html)
consumer/              one consumer image, configured per container via environment variables
docs/superpowers/      design spec and implementation plan
```

Consumer settings (environment variables): `SERVICE_NAME`, `SUBSCRIPTION`, `SUBSCRIPTION_TYPE` (Exclusive | Shared | Failover | KeyShared), `PULSAR_URL`, `TOPIC`, `PROCESSING_MS` (simulated work, default 300).

## Troubleshooting

- **`npm ci` fails with `UNABLE_TO_VERIFY_LEAF_SIGNATURE` or "Exit handler never called" during the build.** Antivirus software (for example Avast Web Shield) or a corporate proxy is intercepting HTTPS. Set `NODE_EXTRA_CA_CERTS` to that software's root certificate (Avast sets it automatically) and rebuild. Compose passes it to the build as a secret; it is never stored in the images.
- **`Bind for 0.0.0.0:3001 failed: port is already allocated`.** Change the host side of the port mapping in `docker-compose.yml` (for example `"3002:3000"`).
- **The services restart a few times at startup.** That's expected if Pulsar is still booting: they exit and Docker restarts them once Pulsar is ready.
