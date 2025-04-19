-- Get the hour of the day with the most orders
SELECT 
    HOUR(order_date) AS hour_of_day,       -- Extract hour (0 to 23) from order timestamp
    COUNT(*) AS order_count                -- Count orders placed in that hour
FROM 
    orders
WHERE 
    order_date IS NOT NULL                 -- Avoid null timestamps
GROUP BY 
    hour_of_day
ORDER BY 
    order_count DESC,                      -- Sort by highest order count
    hour_of_day                            -- Tie-breaker: earlier hour first
LIMIT 1;                                   -- Return the most popular ordering hour

