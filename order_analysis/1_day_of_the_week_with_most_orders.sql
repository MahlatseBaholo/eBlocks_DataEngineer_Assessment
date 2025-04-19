-- Get the most popular day of the week for orders
SELECT 
    DAYNAME(order_date) AS day_of_week,        -- Extract day of the week
    COUNT(*) AS order_count                    -- Count number of orders on that day
FROM 
    orders
WHERE 
    order_date IS NOT NULL                     -- Exclude rows with missing dates
GROUP BY 
    day_of_week
ORDER BY 
    order_count DESC,                          -- Sort by highest order count
    day_of_week                                -- Tie-breaker: alphabetical day
LIMIT 1;                                       -- Get the most frequent day

