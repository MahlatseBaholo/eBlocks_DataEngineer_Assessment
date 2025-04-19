-- Step 1: Assign a row number to each order per customer ordered by order_date
WITH ordered_orders AS (
    SELECT 
        customer_id,
        order_date,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS rn
    FROM 
        orders
    WHERE 
        customer_id IS NOT NULL AND order_date IS NOT NULL
),

-- Step 2: Join each order with the *next* order to calculate time difference
order_intervals AS (
    SELECT 
        o1.customer_id,
        DATEDIFF(o2.order_date, o1.order_date) AS days_between_orders
    FROM 
        ordered_orders o1
    JOIN 
        ordered_orders o2 
        ON o1.customer_id = o2.customer_id AND o2.rn = o1.rn + 1
)

-- Step 3: Calculate average interval per customer and overall average
SELECT 
    ROUND(AVG(days_between_orders), 2) AS avg_days_between_orders
FROM 
    order_intervals;

