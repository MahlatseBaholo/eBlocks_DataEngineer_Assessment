-- Step 1: Get each customer's first order based on order_date
WITH first_orders AS (
    SELECT 
        o.customer_id,
        MIN(o.order_date) AS first_order_date
    FROM 
        orders o
    WHERE 
        o.customer_id IS NOT NULL
        AND o.order_date IS NOT NULL
    GROUP BY 
        o.customer_id
),

-- Step 2: Get product(s) from the order_details of those first orders
first_order_products AS (
    SELECT 
        f.customer_id,
        od.product_id
    FROM 
        first_orders f
    JOIN 
        order_details od ON f.first_order_date = (
            SELECT MIN(o.order_date) 
            FROM orders o 
            WHERE o.id = od.order_id
        )
)

-- Step 3: Count how many times each product appears as the first product bought
SELECT 
    p.product_name,
    fop.product_id,
    COUNT(*) AS times_as_first_product
FROM 
    first_order_products fop
JOIN 
    products p ON p.id = fop.product_id
GROUP BY 
    fop.product_id, p.product_name
ORDER BY 
    times_as_first_product DESC
LIMIT 1;
