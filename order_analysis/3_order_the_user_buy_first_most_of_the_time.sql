-- Step 1: Get first order (smallest order_date) per customer using a subquery
WITH first_orders AS (
    SELECT o.customer_id, o.id AS first_order_id
    FROM orders o
    WHERE o.order_date = (
        SELECT MIN(o2.order_date)
        FROM orders o2
        WHERE o2.customer_id = o.customer_id
    )
),

-- Step 2: Get the products from the first order
first_order_products AS (
    SELECT 
        f.customer_id,
        od.product_id
    FROM 
        first_orders f
    JOIN 
        order_details od ON f.first_order_id = od.order_id
)

-- Step 3: Count how often each product appears as a first purchase
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
