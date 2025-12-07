const { Kafka, Partitioners } = require('kafkajs');
const { Pool } = require('pg');

// --- Configuration ---


// PostgreSQL Configuration (using the same config names as the Order Service)
const DB_CONFIG = {
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    ssl: { rejectUnauthorized: false } 
};

// Topic names to subscribe to
const SUCCESS_TOPIC = 'payment_succeeded';
const FAILURE_TOPIC = 'payment_failed';
const NO_STOCK_TOPIC = 'order_failed_no_stock'; // Also listen to inventory failure

// Initialize Clients
const pgPool = new Pool(DB_CONFIG);

// Kafka Configuration
const KAFKA_BROKERS = process.env.KAFKA_BROKERS ? process.env.KAFKA_BROKERS.split(',') : ['localhost:9092'];
const KAFKA_USERNAME = process.env.KAFKA_USERNAME;
const KAFKA_PASSWORD = process.env.KAFKA_PASSWORD;
const KAFKA_CA_CERT = process.env.KAFKA_CA_CERT; // The secret certificate content

const kafka = new Kafka({
    clientId: 'order-finalizer-service',
    brokers: KAFKA_BROKERS,
    // CRITICAL: SSL configuration using the environment variable CA Cert
    ssl: { 
        rejectUnauthorized: true, 
        ca: KAFKA_CA_CERT ? [Buffer.from(KAFKA_CA_CERT)] : undefined 
    }, 
    // SASL Authentication
    sasl: {
        mechanism: 'scram-sha-256', 
        username: KAFKA_USERNAME,
        password: KAFKA_PASSWORD,
    }
});

const consumer = kafka.consumer({ groupId: 'order-finalizers' });

// Add Express for the mandatory Health Check
const express = require('express');
const healthApp = express();
const HEALTH_PORT = 8080; // App Platform defaults to checking this port

// --- HEALTH CHECK SERVER ---
// Define a simple health check endpoint
healthApp.get('/health', (req, res) => {
    // This confirms the worker process is alive
    res.status(200).send('Payment Worker is alive.');
});

// Start the listener server
healthApp.listen(HEALTH_PORT, () => {
    console.log(`Health Check Server listening on port ${HEALTH_PORT}`);
});

// --- 2. Deployment Steps (5.1) and Finalization Logic (5.2) ---
async function runOrderFinalizer() {
    try {
        await consumer.connect();
        
        // Subscribe to all outcome topics relevant to order status
        await consumer.subscribe({ topic: SUCCESS_TOPIC, fromBeginning: false }); 
        await consumer.subscribe({ topic: FAILURE_TOPIC, fromBeginning: false });
        await consumer.subscribe({ topic: NO_STOCK_TOPIC, fromBeginning: false }); 

        console.log('Order Finalizer connected and subscribed to all outcome topics.');

        await consumer.run({
            eachMessage: async ({ topic, message }) => {
                const orderId = message.key.toString();
                const payload = JSON.parse(message.value.toString());

                console.log(`[START] Finalizing Order: ${orderId} from topic ${topic}`);

                // ----------------------------------------------------
                // --- SUCCESS PATH (5.2) ---
                if (topic === SUCCESS_TOPIC) {
                    const updateQuery = `
                        UPDATE orders SET status = $1, updated_at = NOW() WHERE order_id = $2 AND status = 'PENDING';
                    `;
                    await pgPool.query(updateQuery, ['PROCESSED', orderId]);
                    console.log(`[SUCCESS] Order ${orderId} finalized and status updated to PROCESSED.`);
                } 
                // ----------------------------------------------------
                // --- COMPENSATION PATH (5.3) ---
                else if (topic === FAILURE_TOPIC || topic === NO_STOCK_TOPIC) {
                    const reason = payload.reason || 'Saga failed.';
                    
                    // Mark the order as failed in the Order Service's database
                    const updateQuery = `
                        UPDATE orders SET status = $1, failure_reason = $2, updated_at = NOW() WHERE order_id = $3 AND status = 'PENDING';
                    `;
                    await pgPool.query(updateQuery, ['FAILED', reason, orderId]);
                    
                    console.error(`[COMPENSATION] Order ${orderId} failed due to ${topic}. Status updated to FAILED.`);

                    // NOTE: In a full system, receiving a FAILURE_TOPIC event
                    // might also trigger a *new* compensation worker (e.g., an Inventory Compensation Worker)
                    // if the failure happened mid-saga (e.g., Payment failed *after* Inventory reserved).
                }
                // ----------------------------------------------------
            },
        });
    } catch (error) {
        console.error('CRITICAL ERROR: Order Finalizer failed to start:', error.message);
    }
}

runOrderFinalizer();
