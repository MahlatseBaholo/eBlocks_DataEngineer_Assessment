ALTER TABLE order_details
DROP FOREIGN KEY fk_order_details__orders;

ALTER TABLE order_details
ADD CONSTRAINT fk_order_details__orders
FOREIGN KEY (order_id) REFERENCES orders(id)
ON DELETE CASCADE;

-- Step 1: Get the latest order_date per customer
WITH latest_order_dates AS (
    SELECT 
        customer_id,
        MAX(order_date) AS latest_order_date
    FROM 
        orders
    WHERE 
        customer_id IS NOT NULL
        AND order_date IS NOT NULL
    GROUP BY 
        customer_id
),

-- Step 2: Get all orders that happened on the customer's latest order date
latest_orders AS (
    SELECT 
        o.*
    FROM 
        orders o
    JOIN 
        latest_order_dates lod 
            ON o.customer_id = lod.customer_id 
           AND o.order_date = lod.latest_order_date
),

-- Step 3: Rank those orders per customer on the latest date
ranked_orders AS (
    SELECT 
        id AS order_id,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, order_date
            ORDER BY id ASC   -- Keep the earliest ID
        ) AS rn
    FROM 
        latest_orders
)

-- Step 4: Delete only duplicates (rn > 1)
DELETE FROM orders
WHERE id IN (
    SELECT order_id
    FROM ranked_orders
    WHERE rn > 1
);

