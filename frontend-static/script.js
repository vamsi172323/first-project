// **IMPORTANT**: You must replace this placeholder with the actual public URL 
// of your Catalog Service (Web Service component) after deployment.
const API_URL = "https://catalog-api-xyz.ondigitalocean.app/api/v1/products";

const productListDiv = document.getElementById('product-list');

async function fetchProducts() {
    try {
        const response = await fetch(API_URL);
        
        // Handle HTTP errors (e.g., 404, 500)
        if (!response.ok) {
            throw new Error(`HTTP error! Status: ${response.status}`);
        }
        
        const products = await response.json();
        
        // Clear the loading message
        productListDiv.innerHTML = ''; 

        if (products.length === 0) {
            productListDiv.innerHTML = '<p>No products found in the catalog.</p>';
            return;
        }

        // Loop through the data and render a card for each product
        products.forEach(product => {
            const card = document.createElement('div');
            card.className = 'product-card';
            
            const stockStatus = product.stock_quantity > 0 
                ? `In Stock: ${product.stock_quantity}` 
                : 'Out of Stock';

            card.innerHTML = `
                <h2>${product.name}</h2>
                <p>${product.description}</p>
                <p class="price">$${product.price}</p>
                <p class="stock">Category: ${product.category} | ${stockStatus}</p>
                `;
            
            productListDiv.appendChild(card);
        });

    } catch (error) {
        console.error("Error fetching products:", error);
        productListDiv.innerHTML = `<p style="color: red;">Failed to load catalog. Check API URL and service status.</p>`;
    }
}

// Execute the function when the page loads
fetchProducts();
