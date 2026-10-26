const { Kafka } = require('kafkajs');
const mongoose = require('mongoose');
const http = require('http');

// 1. Create a lightweight HTTP server for Render health checks
const PORT = process.env.PORT || 5000;
let isReady = false;
const httpServer = http.createServer((req, res) => {
  const statusCode = isReady ? 200 : 503;
  res.writeHead(statusCode, {
    'Content-Type': 'text/plain',
    'Cache-Control': 'no-store',
  });
  res.end(isReady ? 'Kafka-DB Integrator Service is ready.\n' : 'Service is starting.\n');
});

httpServer.listen(PORT, () => {
  console.log(`[HTTP Server] Health check server listening on port ${PORT}`);
});

// Environment variables configuration & Diagnostics
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092')
  .split(',')
  .map((broker) => broker.trim())
  .filter(Boolean);
const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017/integration_db';
const KAFKA_USERNAME = process.env.KAFKA_USERNAME;
const KAFKA_PASSWORD = process.env.KAFKA_PASSWORD;

if (!KAFKA_BROKERS.length) {
  throw new Error('KAFKA_BROKERS must contain at least one broker.');
}

const kafkaSasl = KAFKA_USERNAME && KAFKA_PASSWORD
  ? {
      mechanism: 'plain',
      username: KAFKA_USERNAME,
      password: KAFKA_PASSWORD,
    }
  : undefined;

// Initialize Kafka Client & Producer with certificate-verified TLS.
const kafka = new Kafka({
  clientId: 'kafka-db-integrator',
  brokers: KAFKA_BROKERS,
  ssl: true,
  ...(kafkaSasl ? { sasl: kafkaSasl } : {}),
});

const producer = kafka.producer();
let producerConnected = false;
let changeStream;
let shutdownPromise;

async function startService() {
  try {
    console.log('[Info] Initializing Kafka-DB Integration Service...');

    if (Boolean(KAFKA_USERNAME) !== Boolean(KAFKA_PASSWORD)) {
      throw new Error('KAFKA_USERNAME and KAFKA_PASSWORD must both be set when using SASL.');
    }

    // 2. Connect to Kafka Producer
    await producer.connect();
    producerConnected = true;
    console.log('[Kafka Success] Producer connected successfully to Aiven Kafka.');

    // 3. Connect to Database (MongoDB Change Data Capture simulation)
    await mongoose.connect(MONGO_URI);
    console.log('[Database Success] Connected to MongoDB.');

    const db = mongoose.connection;
    const collectionName = 'tracked_records';
    
    // Ensure collection exists for change streams
    const collections = await db.db.listCollections({ name: collectionName }).toArray();
    if (collections.length === 0) {
      await db.createCollection(collectionName);
    }

    changeStream = db.collection(collectionName).watch([], { fullDocument: 'updateLookup' });
    console.log(`[CDC Active] Listening for real-time changes on collection: "${collectionName}"...`);

    // 4. Listen to Database Changes and Stream to Kafka
    changeStream.on('change', async (change) => {
      try {
        const eventPayload = {
          operationType: change.operationType,
          documentKey: change.documentKey,
          fullDocument: change.fullDocument || null,
          timestamp: new Date().toISOString(),
        };

        await producer.send({
          topic: 'database-events-topic',
          messages: [
            { 
              key: change.documentKey ? String(change.documentKey._id) : 'default-key', 
              value: JSON.stringify(eventPayload) 
            }
          ],
        });

        console.log(`[Kafka Stream] Event successfully published: ${change.operationType}`);
      } catch (streamError) {
        console.error('[Kafka Stream Error] Failed to publish change event:', streamError);
      }
    });

    isReady = true;

  } catch (error) {
    console.error('[Fatal Error] Service startup failed:', error);
    process.exitCode = 1;
    await shutdown('startup failure');
  }
}

async function shutdown(reason) {
  if (shutdownPromise) {
    return shutdownPromise;
  }

  isReady = false;
  console.log(`[Info] Shutting down service (${reason})...`);
  shutdownPromise = (async () => {
    const cleanupTasks = [];

    if (changeStream) {
      cleanupTasks.push(() => changeStream.close());
    }
    if (producerConnected) {
      cleanupTasks.push(() => producer.disconnect());
    }
    if (mongoose.connection.readyState !== 0) {
      cleanupTasks.push(() => mongoose.connection.close());
    }

    for (const cleanupTask of cleanupTasks) {
      try {
        await cleanupTask();
      } catch (error) {
        console.error('[Shutdown Error] Resource cleanup failed:', error);
        process.exitCode = 1;
      }
    }

    if (httpServer.listening) {
      await new Promise((resolve) => httpServer.close(resolve));
    }
  })();

  return shutdownPromise;
}

// Start the service
startService();

for (const signal of ['SIGINT', 'SIGTERM']) {
  process.on(signal, () => {
    shutdown(signal).catch((error) => {
      console.error('[Shutdown Error] Failed to shut down cleanly:', error);
      process.exitCode = 1;
    });
  });
}
