-- Retrieve the first order date for each customer
SELECT 
    customer_id,                          -- The unique ID of the customer
    MIN(order_date) AS first_order_date   -- The earliest order date (first purchase)
FROM 
    orders                                -- Source table containing order data
WHERE 
    customer_id IS NOT NULL               -- Ensure customer_id is valid
    AND order_date IS NOT NULL            -- Ensure order_date is present
GROUP BY 
    customer_id                           -- Group results by each customer
ORDER BY 
    first_order_date ASC;                 -- Sort results by the earliest order date
