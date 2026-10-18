// A generic order consumer. The same code runs as inventory-service-1/2,
// notification-service and billing-service; only the environment (subscription name/type) differs.

const Pulsar = require('pulsar-client');

const SERVICE_NAME = process.env.SERVICE_NAME || 'order-consumer';
const PULSAR_URL = process.env.PULSAR_URL || 'pulsar://localhost:6650';
const TOPIC = process.env.TOPIC || 'persistent://public/default/orders';
const SUBSCRIPTION = process.env.SUBSCRIPTION || 'demo';
const SUBSCRIPTION_TYPE = process.env.SUBSCRIPTION_TYPE || 'Exclusive'; // Exclusive | Shared | Failover | KeyShared
const PROCESSING_MS = Number(process.env.PROCESSING_MS || 300); // simulated work per message

const c = {
  reset: '\x1b[0m', bold: '\x1b[1m', dim: '\x1b[2m',
  green: '\x1b[32m', yellow: '\x1b[33m', red: '\x1b[31m',
};
// One colour per service so screenshots are easy to read.
const SERVICE_COLOURS = {
  'inventory-service-1': '\x1b[35m', // magenta
  'inventory-service-2': '\x1b[34m', // blue
  'notification-service': '\x1b[33m', // yellow
  'billing-service': '\x1b[36m', // cyan
};
const colour = SERVICE_COLOURS[SERVICE_NAME] || '\x1b[32m'; // green for any other service
const tag = `${colour}${c.bold}[${SERVICE_NAME} | sub=${SUBSCRIPTION} ${SUBSCRIPTION_TYPE}]${c.reset}`;
const log = (msg) => console.log(`${c.dim}${new Date().toISOString()}${c.reset} ${msg}`);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Keep the Pulsar C++ client quiet: only forward warnings and errors.
const pulsarLog = (level, _file, _line, message) => {
  if (level >= Pulsar.LogLevel.WARN) console.log(`${c.yellow}[pulsar-client]${c.reset} ${message}`);
};

let client;
let consumer;
let running = true;

async function main() {
  client = new Pulsar.Client({ serviceUrl: PULSAR_URL, operationTimeoutSeconds: 30, log: pulsarLog });
  consumer = await client.subscribe({
    topic: TOPIC,
    subscription: SUBSCRIPTION,
    subscriptionType: SUBSCRIPTION_TYPE,
    subscriptionInitialPosition: 'Earliest', // a brand-new subscription starts from the oldest message
    consumerName: SERVICE_NAME,
    receiverQueueSize: 1, // small prefetch so a Shared subscription spreads messages evenly
  });
  log(`${tag} ${c.green}✔ Subscribed${c.reset} to ${c.bold}${TOPIC}${c.reset}, waiting for events...`);

  while (running) {
    let msg;
    try {
      msg = await consumer.receive();
    } catch (err) {
      if (running) log(`${tag} ${c.red}✖ receive failed:${c.reset} ${err.message}`);
      await sleep(1000);
      continue;
    }
    await handle(msg);
  }
}

async function handle(msg) {
  const msgId = msg.getMessageId().toString();
  const redeliveries = msg.getRedeliveryCount();
  let order;
  try {
    order = JSON.parse(msg.getData().toString());
  } catch {
    // Poison message: ack it anyway so it is not redelivered forever.
    log(`${tag} ${c.yellow}⚠ Skipping non-JSON message${c.reset} ${c.dim}msgId=${msgId}${c.reset}`);
    await consumer.acknowledge(msg);
    return;
  }

  const eventType = msg.getProperties().eventType || 'unknown';
  log(`${tag} ${c.bold}📥 RECEIVED${c.reset} ${eventType} order ${c.bold}${String(order.orderId).slice(0, 8)}${c.reset} ` +
      `product=${order.product} qty=${order.quantity} ${c.dim}msgId=${msgId}` +
      `${redeliveries ? ` redelivery=${redeliveries}` : ''}${c.reset}`);

  await sleep(PROCESSING_MS); // pretend to reserve stock / send an e-mail
  await consumer.acknowledge(msg);
  log(`${tag} ${c.green}✅ ACK${c.reset} order ${String(order.orderId).slice(0, 8)}`);
}

async function shutdown() {
  running = false;
  log(`${tag} Shutting down...`);
  await consumer?.close().catch(() => {});
  await client?.close().catch(() => {});
  process.exit(0);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

main().catch((err) => {
  log(`${tag} ${c.red}✖ Could not subscribe:${c.reset} ${err.message}`);
  process.exit(1); // docker compose restarts us
});
