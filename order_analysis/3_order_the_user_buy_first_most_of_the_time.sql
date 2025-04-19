-- Get the first order date for each customer
SELECT 
    customer_id,
    MIN(order_date) AS first_order_date
FROM 
    orders
WHERE 
    customer_id IS NOT NULL
    AND order_date IS NOT NULL
GROUP BY 
    customer_id
ORDER BY 
    first_order_date ASC;
