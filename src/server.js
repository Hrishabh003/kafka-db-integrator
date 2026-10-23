const { Kafka } = require('kafkajs');
const mongoose = require('mongoose');

// Environment variables configuration
const KAFKA_BROKERS = process.env.KAFKA_BROKERS ? process.env.KAFKA_BROKERS.split(',') : ['localhost:9092'];
const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017/integration_db';
const KAFKA_USERNAME = process.env.KAFKA_USERNAME || 'avnadmin';
const KAFKA_PASSWORD = process.env.KAFKA_PASSWORD || '';

// Initialize Kafka Client & Producer with SSL & SASL for Aiven
const kafka = new Kafka({
  clientId: 'kafka-db-integrator',
  brokers: KAFKA_BROKERS,
  ssl: {
    rejectUnauthorized: false,
  },
  sasl: {
    mechanism: 'scram-sha-256',
    username: KAFKA_USERNAME,
    password: KAFKA_PASSWORD,
  }
});

const producer = kafka.producer();

async function startService() {
  try {
    console.log('[Info] Initializing Kafka-DB Integration Service...');

    // 1. Connect to Kafka Producer
    await producer.connect();
    console.log('[Kafka Success] Producer connected successfully to Aiven Kafka.');

    // 2. Connect to Database (MongoDB Change Data Capture simulation)
    await mongoose.connect(MONGO_URI);
    console.log('[Database Success] Connected to MongoDB.');

    const db = mongoose.connection;
    const collectionName = 'tracked_records';
    
    // Ensure collection exists for change streams
    const collections = await db.db.listCollections({ name: collectionName }).toArray();
    if (collections.length === 0) {
      await db.createCollection(collectionName);
    }

    const changeStream = db.collection(collectionName).watch();
    console.log(`[CDC Active] Listening for real-time changes on collection: "${collectionName}"...`);

    // 3. Listen to Database Changes and Stream to Kafka
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

  } catch (error) {
    console.error('[Fatal Error] Service startup failed:', error);
    process.exit(1);
  }
}

// Graceful shutdown handling
process.on('SIGINT', async () => {
  console.log('\n[Info] Shutting down service gracefully...');
  await producer.disconnect();
  await mongoose.connection.close();
  process.exit(0);
});

// Start the service
startService();
