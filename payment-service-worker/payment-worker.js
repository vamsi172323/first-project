const { Kafka, Partitioners } = require('kafkajs');

// --- Configuration from Environment Variables ---
// (Requires the same KAFKA_BROKERS, KAFKA_USERNAME, KAFKA_PASSWORD, KAFKA_CA_CERT variables)
const KAFKA_BROKERS = process.env.KAFKA_BROKERS ? process.env.KAFKA_BROKERS.split(',') : ['localhost:9092'];
const KAFKA_USERNAME = process.env.KAFKA_USERNAME;
const KAFKA_PASSWORD = process.env.KAFKA_PASSWORD;
const KAFKA_CA_CERT = process.env.KAFKA_CA_CERT; 

// Topic names
const INPUT_TOPIC = 'inventory_reserved'; // Consumes the success event from M3
const SUCCESS_TOPIC = 'payment_succeeded';
const FAILURE_TOPIC = 'payment_failed';

// 2. Initialize Kafka Clients
const kafka = new Kafka({
    clientId: 'payment-service',
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

const consumer = kafka.consumer({ groupId: 'payment-processors' });
const producer = kafka.producer({
    createPartitioner: Partitioners.DefaultPartitioner,
});

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
// -----------------------------
// ... Rest of the Kafka worker logic follows ...

// --- 3. Core Worker Logic ---
async function runPaymentWorker() {
    try {
        await producer.connect();
        await consumer.connect();
        
        // Subscribe to the topic that signals Inventory is reserved
        await consumer.subscribe({ topic: INPUT_TOPIC, fromBeginning: false }); 
        console.log('Payment Worker connected and subscribed to inventory_reserved.');

        await consumer.run({
            autoCommit: true,
            eachMessage: async ({ message }) => {
                const orderId = message.key.toString();
                const payload = JSON.parse(message.value.toString());
                const { totalAmount } = payload;
                
                let paymentSuccessful = true;
                
                console.log(`[START] Processing Payment for Order: ${orderId}. Amount: ${totalAmount}`);

                // --- Payment Simulation (Task 4.3) ---
                // Simulating a 0.1% chance of failure for testing the compensation path
                if (Math.random() < 0.001) { 
                    paymentSuccessful = false;
                    console.error(`[FAIL] Simulated payment failure for Order ${orderId}.`);
                } 
                // CRITICAL: In a real system, you would call an external Stripe/PayPal API here.
                // The external API call would block until completion.
                
                // --- Publish Result Event ---
                if (paymentSuccessful) {
                    // Success Path: Publish event to finalize the order
                    await producer.send({
                        topic: SUCCESS_TOPIC,
                        messages: [{ key: orderId, value: JSON.stringify({ orderId, status: 'PROCESSED', totalAmount }) }],
                    });
                    console.log(`[SUCCESS] Payment processed for Order ${orderId}. Published to ${SUCCESS_TOPIC}.`);

                } else {
                    // Failure Path: Publish compensation event
                    const reason = "External payment gateway declined transaction.";
                    await producer.send({
                        topic: FAILURE_TOPIC,
                        messages: [{ 
                            key: orderId, 
                            value: JSON.stringify({ orderId, reason, service: 'payment' }) 
                        }],
                    });
                    console.log(`[FAIL] Payment declined for Order ${orderId}. Published to ${FAILURE_TOPIC}.`);
                }
            },
        });
    } catch (error) {
        console.error('CRITICAL ERROR: Payment Worker failed to start or run:', error.message);
    }
}

runPaymentWorker();
