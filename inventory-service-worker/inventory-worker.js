const { Kafka, Partitioners } = require('kafkajs');
const { Pool } = require('pg');

// --- 1. Configuration from Environment Variables ---
// All variables must be set in the DigitalOcean App Platform Worker Component

// Kafka Configuration
const KAFKA_BROKERS = process.env.KAFKA_BROKERS ? process.env.KAFKA_BROKERS.split(',') : ['localhost:9092'];
const KAFKA_USERNAME = process.env.KAFKA_USERNAME;
const KAFKA_PASSWORD = process.env.KAFKA_PASSWORD;
const KAFKA_CA_CERT = process.env.KAFKA_CA_CERT; // The secret certificate content

// PostgreSQL Configuration (using the same config names as the Order Service)
const DB_CONFIG = {
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    ssl: { rejectUnauthorized: false } 
};

// Topic names (Input and Outputs for the Saga)
const INPUT_TOPIC = 'raw_orders';
const SUCCESS_TOPIC = 'inventory_reserved'; // New topic for success
const FAILURE_TOPIC = 'order_failed_no_stock'; // New topic for compensation

// 2. Initialize Clients
const pgPool = new Pool(DB_CONFIG);

const kafka = new Kafka({
    clientId: 'inventory-service',
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

// A persistent Group ID is required for consumer parallelism
const consumer = kafka.consumer({ groupId: 'inventory-processors' });
const producer = kafka.producer({
    createPartitioner: Partitioners.DefaultPartitioner,
});


// --- 3. Core Worker Logic ---
async function runInventoryWorker() {
    try {
        await producer.connect();
        console.log('Kafka Producer connected successfully.');

        await consumer.connect();
        console.log('Kafka Consumer connected successfully.');
        
        // Subscribe to the input topic for the Inventory domain
        await consumer.subscribe({ topic: INPUT_TOPIC, fromBeginning: false }); 

        await consumer.run({
            // Ensure the worker only commits the offset AFTER the processing is complete
            autoCommit: true, 
            eachMessage: async ({ message }) => {
                const orderId = message.key.toString(); // OrderId is the message key
                const payload = JSON.parse(message.value.toString());
                const { items, userId } = payload;
                
                let pgClient;
                let deductionSuccessful = true;
                
                console.log(`[START] Processing Order: ${orderId} for inventory deduction.`);

                try {
                    pgClient = await pgPool.connect();
                    await pgClient.query('BEGIN'); // Start local ACID transaction

                    // Iterate through items and perform the deduction check and update
                    for (const item of items) {
                        const { productId, quantity } = item;
                        
                        // CRITICAL: SELECT FOR UPDATE locks the rows to prevent race conditions 
                        // when multiple workers are running, even though Kafka partitions help.
                        const stockCheckQuery = `
                            SELECT stock_quantity 
                            FROM inventory 
                            WHERE product_id = $1 
                            FOR UPDATE;
                        `;
                        const result = await pgClient.query(stockCheckQuery, [productId]);

                        if (result.rows.length === 0) {
                            deductionSuccessful = false;
                            throw new Error(`Product ${productId} not found.`);
                        }
                        
                        const { stock_quantity } = result.rows[0];
                        if (stock_quantity < quantity) {
                            deductionSuccessful = false;
                            throw new Error(`Insufficient stock for product ${productId}. Available: ${stock_quantity}, Requested: ${quantity}`);
                        }

                        // Perform the atomic stock deduction
                        const updateQuery = `
                            UPDATE inventory 
                            SET stock_quantity = stock_quantity - $1 
                            WHERE product_id = $2;
                        `;
                        await pgClient.query(updateQuery, [quantity, productId]);
                    }

                    await pgClient.query('COMMIT'); // Transaction successful!
                    console.log(`[SUCCESS] Inventory deducted for Order ${orderId}. Moving to next step.`);

                } catch (error) {
                    // --- Compensation Path (Task 3.4) ---
                    if (pgClient) {
                        await pgClient.query('ROLLBACK'); // Undo any partial changes
                    }
                    deductionSuccessful = false;
                    
                    console.error(`[FAIL] Deduction rolled back for Order ${orderId}. Reason:`, error.message);

                    // Publish the compensation event to the Order Service
                    await producer.send({
                        topic: FAILURE_TOPIC,
                        messages: [{ 
                            key: orderId, 
                            value: JSON.stringify({ 
                                orderId, 
                                userId,
                                reason: error.message,
                                service: 'inventory'
                            }) 
                        }],
                    });
                    // IMPORTANT: The autoCommit=true will still commit the offset here, 
                    // marking the message as handled (even if it failed).
                } finally {
                    if (pgClient) pgClient.release();
                }

                // --- Success Path (Task 3.3) ---
                if (deductionSuccessful) {
                    // Publish the success event to trigger the next Saga step (Payment Service)
                    await producer.send({
                        topic: SUCCESS_TOPIC,
                        messages: [{ key: orderId, value: JSON.stringify(payload) }],
                    });
                }
            },
        });
    } catch (error) {
        console.error('CRITICAL ERROR: Inventory Worker failed to start or run:', error.message);
    }
}

runInventoryWorker();
