const express = require('express');
const { Pool } = require('pg');

// 1. Database Connection Pool Setup
const pool = new Pool({
    // DigitalOcean App Platform will automatically map these environment variables
    // when you link your Managed PostgreSQL database.
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    ssl: { rejectUnauthorized: false } // Required for secure DO connections
});

const app = express();
const PORT = process.env.PORT || 8080; // App Platform requires listening on 8080

// Middleware to allow cross-origin requests from your static site
app.use(express.json());
app.use((req, res, next) => {
    // In a real app, replace '*' with your specific Frontend URL
    res.setHeader('Access-Control-Allow-Origin', '*'); 
    next();
});

// 2. Main Catalog API Endpoint
app.get('/api/v1/products', async (req, res) => {
    try {
        const query = `
            SELECT 
                p.product_id, p.name, p.price, p.category, 
                i.stock_quantity, i.reserved_quantity
            FROM products p
            JOIN inventory i ON p.product_id = i.product_id
            WHERE i.stock_quantity > 0; -- Only show items with stock
        `;
        const { rows } = await pool.query(query);
        res.status(200).json(rows);
    } catch (err) {
        console.error('Database query error:', err);
        res.status(500).json({ error: 'Failed to fetch catalog data' });
    }
});

// 3. Health Check
app.get('/health', (req, res) => {
    res.status(200).send('Catalog Service is running!');
});

app.listen(PORT, () => {
    console.log(`Catalog Service listening on port ${PORT}`);
});
