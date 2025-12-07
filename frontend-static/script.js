// --- CRITICAL CONFIGURATION ---
// REPLACE WITH YOUR DEPLOYED CATALOG SERVICE URL
const CATALOG_SERVICE_URL = 'https://orca-app-cbmvq.ondigitalocean.app/api/v1/products'; 
// REPLACE WITH YOUR DEPLOYED ORDER SERVICE URL
const ORDER_SERVICE_URL = 'https://plankton-app-e3aes.ondigitalocean.app/api/v1/orders/place'; 

const productListElement = document.getElementById('product-list');
const statusMessageElement = document.getElementById('order-status-message');

/**
 * Fetches products and renders them with an 'Order' button.
 */
async function fetchAndRenderProducts() {
    try {
        const response = await fetch(CATALOG_SERVICE_URL);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const products = await response.json();
        
        productListElement.innerHTML = ''; // Clear previous content

        products.forEach(product => {
            const priceDisplay = parseFloat(product.price).toFixed(2);
            const card = document.createElement('div');
            card.className = 'product-card';
            card.innerHTML = `
                <h3>${product.name}</h3>
                <p>Price: $${priceDisplay}</p>
                <p>Stock: ${product.stock_quantity}</p>
                <button 
                    data-product-id="${product.product_id}" 
                    data-product-name="${product.name}" 
                    data-price="${product.price}"
                    onclick="handleOrderClick(this)">
                    Order 1 Unit
                </button>
            `;
            productListElement.appendChild(card);
        });

    } catch (error) {
        productListElement.innerHTML = `<p style="color: red;">Failed to load products: ${error.message}</p>`;
    }
}

/**
 * Handles the click event on the order button and initiates the Saga via the Order Service API.
 * @param {HTMLElement} button - The clicked button element.
 */
function handleOrderClick(button) {
    const productId = button.getAttribute('data-product-id');
    const productName = button.getAttribute('data-product-name');
    const unitPrice = parseFloat(button.getAttribute('data-price'));
    const quantity = 1; // Always order 1 unit for testing

    statusMessageElement.textContent = `Placing order for ${productName}... Saga starting.`;
    statusMessageElement.style.color = 'orange';
    
    const orderData = {
        userId: "11223344-5566-7788-99aa-bbccddeeff00", // Hardcoded test user ID
        totalAmount: unitPrice * quantity,
        items: [
            { 
                productId: productId, 
                productName: productName, 
                unitPrice: unitPrice, 
                quantity: quantity 
            }
        ]
    };

    fetch(ORDER_SERVICE_URL, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(orderData),
    })
    .then(response => {
        if (response.status === 202) {
            statusMessageElement.textContent = `Order accepted (202) for ${productName}. Check worker logs for final status.`;
            statusMessageElement.style.color = 'green';
        } else {
            statusMessageElement.textContent = `Order placement failed. Status: ${response.status}`;
            statusMessageElement.style.color = 'red';
        }
    })
    .catch(error => {
        console.error('Error placing order:', error);
        statusMessageElement.textContent = `Error connecting to Order Service.`;
        statusMessageElement.style.color = 'red';
    });
}

// Initial call to load products when the page loads
document.addEventListener('DOMContentLoaded', fetchAndRenderProducts);
