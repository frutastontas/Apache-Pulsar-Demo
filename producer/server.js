// order-service: an Express app that PUBLISHES "OrderCreated" events to Pulsar.
// It only knows the topic name; it has no idea which services consume the events.

const path = require('path');
const crypto = require('crypto');
const express = require('express');
const Pulsar = require('pulsar-client');

const PORT = Number(process.env.PORT || 4000);
const PULSAR_URL = process.env.PULSAR_URL || 'pulsar://localhost:6650';
const PULSAR_ADMIN_URL = process.env.PULSAR_ADMIN_URL || 'http://localhost:8080';
const TOPIC = process.env.TOPIC || 'persistent://public/default/orders';
const TRAFFIC_RATE = Number(process.env.TRAFFIC_RATE || 10); // orders per second while traffic is on
// Admin REST path for the topic: persistent://public/default/orders -> persistent/public/default/orders
const TOPIC_ADMIN_PATH = `${PULSAR_ADMIN_URL}/admin/v2/${TOPIC.replace('://', '/')}`;

const PRODUCTS = ['Laptop', 'Keyboard', 'Mouse', 'Monitor', 'Headphones', 'Webcam', 'USB-C Hub'];

const c = {
  reset: '\x1b[0m', bold: '\x1b[1m', dim: '\x1b[2m',
  green: '\x1b[32m', yellow: '\x1b[33m', cyan: '\x1b[36m', red: '\x1b[31m',
};
const topicName = TOPIC.split('/').pop();
const log = (msg) => console.log(`${c.dim}${new Date().toISOString()}${c.reset} ${msg}`);

// Keep the Pulsar C++ client quiet: only forward warnings and errors.
const pulsarLog = (level, _file, _line, message) => {
  if (level >= Pulsar.LogLevel.WARN) console.log(`${c.yellow}[pulsar-client]${c.reset} ${message}`);
};

let client;
let producer;

async function connect() {
  client = new Pulsar.Client({ serviceUrl: PULSAR_URL, operationTimeoutSeconds: 30, log: pulsarLog });
  producer = await client.createProducer({
    topic: TOPIC,
    producerName: 'order-service',
    sendTimeoutMs: 10000,
    batchingEnabled: false, // one message per send, so each order gets its own message ID
  });
  log(`${c.green}✔ Connected to Pulsar${c.reset} at ${PULSAR_URL}, producing to ${c.bold}${TOPIC}${c.reset}`);
}

async function publishOrder(product, quantity) {
  const event = {
    orderId: crypto.randomUUID(),
    product,
    quantity,
    createdAt: new Date().toISOString(),
  };
  const messageId = await producer.send({
    data: Buffer.from(JSON.stringify(event)),
    partitionKey: event.orderId,
    properties: { eventType: 'OrderCreated' },
  });
  const msgId = messageId.toString();
  log(`${c.cyan}📤 SENT${c.reset} order ${c.bold}${event.orderId.slice(0, 8)}${c.reset} ` +
      `product=${event.product} qty=${event.quantity} → ${c.bold}${topicName}${c.reset} ${c.dim}msgId=${msgId}${c.reset}`);
  return { orderId: event.orderId, messageId: msgId, event };
}

function publishRandomOrder() {
  const product = PRODUCTS[Math.floor(Math.random() * PRODUCTS.length)];
  return publishOrder(product, 1 + Math.floor(Math.random() * 5));
}

// --- Traffic generator: publishes TRAFFIC_RATE orders/s until stopped -------
const traffic = { running: false, sent: 0, timer: null };

function startTraffic() {
  if (traffic.running) return;
  traffic.running = true;
  traffic.sent = 0;
  log(`${c.yellow}🚦 Traffic ON${c.reset} (${TRAFFIC_RATE} orders/s)`);
  traffic.timer = setInterval(() => {
    publishRandomOrder()
      .then(() => { traffic.sent++; })
      .catch((err) => log(`${c.red}✖ Failed to publish:${c.reset} ${err.message}`));
  }, 1000 / TRAFFIC_RATE);
}

function stopTraffic() {
  if (!traffic.running) return;
  clearInterval(traffic.timer);
  traffic.running = false;
  log(`${c.yellow}🚦 Traffic OFF${c.reset} after ${traffic.sent} orders`);
}

// --- Live stats: poll the Pulsar admin API every second ----------------------
// Pulsar's own msgRateIn/Out are only refreshed periodically, so rates are
// computed here from the (real-time) counters between two polls.
let stats = { error: 'not polled yet' };
let previous = null;

async function pollStats() {
  try {
    const res = await fetch(`${TOPIC_ADMIN_PATH}/stats`);
    if (!res.ok) throw new Error(`Pulsar admin API returned HTTP ${res.status}`);
    const s = await res.json();
    const now = Date.now();
    const seconds = previous ? (now - previous.at) / 1000 : 0;
    const perSec = (current, before) =>
      seconds > 0 && before !== undefined ? Math.max(0, (current - before) / seconds) : 0;

    const subscriptions = Object.entries(s.subscriptions).map(([name, sub]) => ({
      name,
      type: sub.type,
      consumers: sub.consumers.length,
      backlog: sub.msgBacklog,
      deliveredPerSec: perSec(sub.msgOutCounter, previous?.delivered[name]),
    }));
    stats = {
      published: s.msgInCounter,
      publishedPerSec: perSec(s.msgInCounter, previous?.published),
      subscriptions,
    };
    previous = {
      at: now,
      published: s.msgInCounter,
      delivered: Object.fromEntries(Object.entries(s.subscriptions).map(([name, sub]) => [name, sub.msgOutCounter])),
    };
  } catch (err) {
    stats = { error: err.message };
    previous = null;
  }
}

function validate(body) {
  const product = typeof body?.product === 'string' ? body.product.trim() : '';
  const quantity = Number(body?.quantity);
  if (!product) return { error: 'product is required' };
  if (!Number.isInteger(quantity) || quantity < 1) return { error: 'quantity must be a positive integer' };
  return { product, quantity };
}

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

app.post('/orders', async (req, res) => {
  const input = validate(req.body);
  if (input.error) return res.status(400).json({ error: input.error });
  try {
    res.status(201).json(await publishOrder(input.product, input.quantity));
  } catch (err) {
    log(`${c.red}✖ Failed to publish:${c.reset} ${err.message}`);
    res.status(500).json({ error: `Failed to publish: ${err.message}` });
  }
});

app.post('/orders/random', async (req, res) => {
  const count = Math.min(Math.max(Number(req.query.count) || 10, 1), 100);
  const sent = [];
  try {
    for (let i = 0; i < count; i++) sent.push(await publishRandomOrder());
    res.status(201).json({ sent });
  } catch (err) {
    log(`${c.red}✖ Failed to publish:${c.reset} ${err.message}`);
    res.status(500).json({ error: `Failed to publish: ${err.message}`, sent });
  }
});

app.post('/api/traffic', (req, res) => {
  const action = req.body?.action;
  if (action === 'start') startTraffic();
  else if (action === 'stop') stopTraffic();
  else return res.status(400).json({ error: 'action must be "start" or "stop"' });
  res.json({ running: traffic.running, sent: traffic.sent, rate: TRAFFIC_RATE });
});

app.get('/api/stats', (req, res) => {
  res.json({ ...stats, traffic: { running: traffic.running, sent: traffic.sent, rate: TRAFFIC_RATE } });
});

// Rewind a subscription's cursor in time: its consumers re-receive those messages.
app.post('/api/replay', async (req, res) => {
  const subscription = String(req.body?.subscription || '');
  const minutes = Number(req.body?.minutes);
  if (!stats.subscriptions?.some((s) => s.name === subscription)) {
    return res.status(400).json({ error: `unknown subscription "${subscription}"` });
  }
  if (!Number.isInteger(minutes) || minutes < 1 || minutes > 60) {
    return res.status(400).json({ error: 'minutes must be an integer between 1 and 60' });
  }
  const timestamp = Date.now() - minutes * 60_000;
  try {
    const r = await fetch(
      `${TOPIC_ADMIN_PATH}/subscription/${encodeURIComponent(subscription)}/resetcursor/${timestamp}`,
      { method: 'POST' },
    );
    if (!r.ok) throw new Error(`Pulsar admin API returned HTTP ${r.status}: ${await r.text()}`);
    // The subscription's backlog right after the reset = events that will be re-delivered.
    const s = await (await fetch(`${TOPIC_ADMIN_PATH}/stats`)).json();
    const replaying = s.subscriptions[subscription]?.msgBacklog ?? 0;
    log(`${c.yellow}⏪ REPLAY${c.reset} subscription ${c.bold}${subscription}${c.reset} rewound ${minutes} min ` +
        `(to ${new Date(timestamp).toISOString()}): ${replaying} events to re-deliver`);
    res.json({ subscription, minutes, from: new Date(timestamp).toISOString(), replaying });
  } catch (err) {
    log(`${c.red}✖ Replay failed:${c.reset} ${err.message}`);
    res.status(502).json({ error: `Replay failed: ${err.message}` });
  }
});

let server;
let statsTimer;

async function shutdown() {
  log('Shutting down order-service...');
  stopTraffic();
  clearInterval(statsTimer);
  server?.close();
  await producer?.close().catch(() => {});
  await client?.close().catch(() => {});
  process.exit(0);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

connect()
  .then(() => {
    statsTimer = setInterval(pollStats, 1000);
    server = app.listen(PORT, () => log(`${c.green}✔ order-service listening${c.reset} on http://localhost:${PORT}`));
  })
  .catch((err) => {
    log(`${c.red}✖ Could not connect to Pulsar:${c.reset} ${err.message}`);
    process.exit(1); // docker compose restarts us
  });
