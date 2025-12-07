const { Kafka } = require('kafkajs');
const { Pool } = require('pg');
const express = require('express');

// --- 1. Configuration and Health Check Setup ---

// Express for Health Check
const healthApp = express();
const HEALTH_PORT = 8080; 

healthApp.get('/health', (req, res) => {
    // Simple check: if the process is running, we are alive.
    res.status(200).send('Order Finalizer Worker is alive.');
});

healthApp.listen(HEALTH_PORT, () => {
    console.log(`Health Check Server listening on port ${HEALTH_PORT}`);
});

// Kafka Configuration (Ensure these environment variables are set as Secrets)
const KAFKA_BROKERS = process.env.KAFKA_BROKERS ? process.env.KAFKA_BROKERS.split(',') : ['localhost:9092'];
const KAFKA_USERNAME = process.env.KAFKA_USERNAME;
const KAFKA_PASSWORD = process.env.KAFKA_PASSWORD;
const KAFKA_CA_CERT = process.env.KAFKA_CA_CERT; 

// PostgreSQL Configuration
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
const NO_STOCK_TOPIC = 'order_failed_no_stock'; 
// NOTE: We also need a topic for releasing inventory when payment fails,
// but we only listen here for the final status update.

// 2. Initialize Clients
const pgPool = new Pool(DB_CONFIG);

const kafka = new Kafka({
    clientId: 'order-finalizer-service',
    brokers: KAFKA_BROKERS,
    ssl: { 
        rejectUnauthorized: true, 
        ca: KAFKA_CA_CERT ? [Buffer.from(KAFKA_CA_CERT)] : undefined 
    }, 
    sasl: {
        mechanism: 'scram-sha-256', 
        username: KAFKA_USERNAME,
        password: KAFKA_PASSWORD,
    }
});

// Use a persistent Group ID for tracking offsets
const consumer = kafka.consumer({ groupId: 'order-finalizers' }); 

// --- 3. Core Worker Logic ---
async function runOrderFinalizer() {
    try {
        await consumer.connect();
        
        // Subscribe to all outcome topics relevant to order status
        await consumer.subscribe({ topic: SUCCESS_TOPIC, fromBeginning: false }); 
        await consumer.subscribe({ topic: FAILURE_TOPIC, fromBeginning: false });
        await consumer.subscribe({ topic: NO_STOCK_TOPIC, fromBeginning: false }); 

        console.log('Order Finalizer connected and subscribed to all outcome topics.');

        await consumer.run({
            autoCommit: true,
            eachMessage: async ({ topic, message }) => {
                const orderId = message.key.toString();
                const payload = JSON.parse(message.value.toString());

                console.log(`[START] Finalizing Order: ${orderId} from topic ${topic}`);
                
                let client;
                try {
                    client = await pgPool.connect();

                    // --- SUCCESS PATH ---
                    if (topic === SUCCESS_TOPIC) {
                        const updateQuery = `
                            UPDATE orders SET status = $1, updated_at = NOW() WHERE order_id = $2 AND status = 'PENDING';
                        `;
                        // Status: PROCESSED, COMPLETED, or FULFILLED depending on business rules
                        const result = await client.query(updateQuery, ['PROCESSED', orderId]);
                        
                        if (result.rowCount === 1) {
                            console.log(`[SUCCESS] Order ${orderId} finalized and status updated to PROCESSED.`);
                        } else {
                            console.warn(`[WARNING] Order ${orderId} not updated. Status was not PENDING.`);
                        }
                    } 
                    
                    // --- COMPENSATION PATH (FAILURE) ---
                    else if (topic === FAILURE_TOPIC || topic === NO_STOCK_TOPIC) {
                        const reason = payload.reason || 'Saga failed due to unknown error.';
                        
                        // Mark the order as failed in the Order Service's database
                        const updateQuery = `
                            UPDATE orders SET status = $1, failure_reason = $2, updated_at = NOW() WHERE order_id = $3 AND status = 'PENDING';
                        `;
                        const result = await client.query(updateQuery, ['FAILED', reason, orderId]);
                        
                        if (result.rowCount === 1) {
                            console.error(`[COMPENSATION] Order ${orderId} failed due to ${topic}. Status updated to FAILED.`);
                        } else {
                            console.warn(`[WARNING] Failed Order ${orderId} not updated. Status was not PENDING.`);
                        }
                    }

                } catch (error) {
                    console.error(`[DB ERROR] Failed to update order status for ${orderId}:`, error.message);
                } finally {
                    if (client) client.release();
                }
            },
        });
    } catch (error) {
        console.error('CRITICAL ERROR: Order Finalizer failed to start:', error.message);
    }
}

runOrderFinalizer();
