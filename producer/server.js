// order-service: an Express app that PUBLISHES "OrderCreated" events to Pulsar.
// It only knows the topic name; it has no idea which services consume the events.

const path = require('path');
const crypto = require('crypto');
const express = require('express');
const Pulsar = require('pulsar-client');

const PORT = Number(process.env.PORT || 4000);
const PULSAR_URL = process.env.PULSAR_URL || 'pulsar://localhost:6650';
const TOPIC = process.env.TOPIC || 'persistent://public/default/orders';

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
    for (let i = 0; i < count; i++) {
      const product = PRODUCTS[Math.floor(Math.random() * PRODUCTS.length)];
      sent.push(await publishOrder(product, 1 + Math.floor(Math.random() * 5)));
    }
    res.status(201).json({ sent });
  } catch (err) {
    log(`${c.red}✖ Failed to publish:${c.reset} ${err.message}`);
    res.status(500).json({ error: `Failed to publish: ${err.message}`, sent });
  }
});

let server;

async function shutdown() {
  log('Shutting down order-service...');
  server?.close();
  await producer?.close().catch(() => {});
  await client?.close().catch(() => {});
  process.exit(0);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

connect()
  .then(() => {
    server = app.listen(PORT, () => log(`${c.green}✔ order-service listening${c.reset} on http://localhost:${PORT}`));
  })
  .catch((err) => {
    log(`${c.red}✖ Could not connect to Pulsar:${c.reset} ${err.message}`);
    process.exit(1); // docker compose restarts us
  });
