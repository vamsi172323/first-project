const express = require('express');
const { Pool } = require('pg');
const { Kafka, Partitioners } = require('kafkajs');
const { v4: uuidv4 } = require('uuid');

const app = express();
const PORT = process.env.PORT || 8080; 

// --- Configuration from Environment Variables ---
// Kafka Configuration
const KAFKA_BROKERS = process.env.KAFKA_BROKERS ? process.env.KAFKA_BROKERS.split(',') : ['localhost:9092'];
const KAFKA_USERNAME = process.env.KAFKA_USERNAME;
const KAFKA_PASSWORD = process.env.KAFKA_PASSWORD;
const KAFKA_CA_CERT = process.env.KAFKA_CA_CERT;
const KAFKA_TOPIC = 'raw_orders';

// PostgreSQL Configuration
const DB_CONFIG = {
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    ssl: { rejectUnauthorized: false } 
};

// 1. Initialize Clients (PostgreSQL Pool and Kafka Producer)
const pgPool = new Pool(DB_CONFIG);

const kafka = new Kafka({
    clientId: 'order-service',
    brokers: KAFKA_BROKERS,
    // DigitalOcean Managed Kafka uses SASL/SCRAM authentication over TLS/SSL
    ssl: { 
        rejectUnauthorized: true, // Recommended for production
        ca: [Buffer.from(KAFKA_CA_CERT)] // Use the certificate from the environment variable
    },
    sasl: {
        mechanism: 'scram-sha-256', 
        username: KAFKA_USERNAME,
        password: KAFKA_PASSWORD,
    }
});

const producer = kafka.producer({
    createPartitioner: Partitioners.DefaultPartitioner,
    allowAutoTopicCreation: false // Ensure topic exists
});

const corsOptions = {
    // Replace the placeholder with the actual URL of your deployed Frontend Service
    // IMPORTANT: Do NOT include a trailing slash (e.g., /)
    origin: 'https://frontend-app-8nidl.ondigitalocean.app
', 
    methods: 'GET,POST',
    allowedHeaders: 'Content-Type',
};

// Connect to Kafka on startup
async function connectKafka() {
    try {
        await producer.connect();
        console.log('Kafka Producer connected successfully.');
    } catch (error) {
        console.error('Failed to connect Kafka Producer:', error.message);
        // Exit or implement sophisticated retry logic in production
    }
}
connectKafka();

// --- Express Middleware & API Endpoint ---
app.use(cors(corsOptions));
app.use(express.json()); // for parsing application/json

/**
 * Task 2.2 & 2.3: Accepts an order, writes to DB, and sends a Kafka message.
 * POST /api/v1/orders/place
 */
app.post('/api/v1/orders/place', async (req, res) => {
    // ⚠️ TODO: Implement robust input validation later
    const { userId, items, totalAmount } = req.body;
    
    if (!userId || !items || items.length === 0 || !totalAmount) {
        return res.status(400).json({ message: 'Missing required order fields.' });
    }

    const orderId = uuidv4(); 
    let pgClient;

    try {
        // --- 1. Transactional Write (PostgreSQL) ---
        pgClient = await pgPool.connect();
        await pgClient.query('BEGIN'); // Start Transaction

        // Insert into orders table (Status: PENDING)
        const orderQuery = `
            INSERT INTO orders (order_id, user_id, total_amount, status)
            VALUES ($1, $2, $3, 'PENDING')
            RETURNING order_id;
        `;
        await pgClient.query(orderQuery, [orderId, userId, totalAmount]);
        
        // Insert order items
        for (const item of items) {
            const itemQuery = `
                INSERT INTO order_items (order_id, product_id, product_name, unit_price, quantity)
                VALUES ($1, $2, $3, $4, $5);
            `;
            await pgClient.query(itemQuery, [
                orderId, 
                item.productId, 
                item.productName, 
                item.unitPrice, 
                item.quantity
            ]);
        }
        
        // Commit the database changes: The Order is now durably PENDING
        await pgClient.query('COMMIT'); 

        // --- 2. Asynchronous Publish (Kafka) ---
        const eventMessage = {
            orderId: orderId,
            userId: userId,
            items: items,
            timestamp: new Date().toISOString()
        };

        await producer.send({
            topic: KAFKA_TOPIC,
            // Use orderId as the message Key for horizontal scaling (ensures all events for one order go to the same partition)
            messages: [{ key: orderId, value: JSON.stringify(eventMessage) }], 
            acks: 1 // Wait for broker leader acknowledgment
        });

        // Response immediately, without waiting for the Saga to complete
        res.status(202).json({ 
            message: 'Order accepted and processing initiated.', 
            orderId: orderId,
            status: 'PENDING'
        });

    } catch (error) {
        // Rollback transaction on any failure
        if (pgClient) {
            await pgClient.query('ROLLBACK');
        }
        console.error('Order placement failed:', error.message);
        res.status(500).json({ message: 'Internal server error: Order failed to submit.' });
    } finally {
        if (pgClient) {
            pgClient.release();
        }
    }
});

app.listen(PORT, () => {
    console.log(`Order Service listening on port ${PORT}`);
});
