/*
This part will compare the actual vs predicted delivery time for products accross different regions to identify and
problem areas where orders are taking too long all the time
The presence of

*/

SELECT
    s.seller_state AS origin,
    c.customer_state AS destination,
    -- The difference between actual and predicted delivery time with + values showing lateness and - the opposite
    ROUND(AVG(date_diff('day', o.order_estimated_delivery_date, o.order_delivered_customer_date)), 1) AS avg_days_late,
    -- The percent of orders that are late per route
    ROUND(
        (COUNT(CASE WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN 1 END) * 100.0) / COUNT(*),
        1
    ) AS late_percent,
    COUNT(*) AS total_orders
FROM orders o
JOIN customers c
    ON o.customer_id = c.customer_id
JOIN order_items oi
    ON o.order_id = oi.order_id
JOIN sellers s
    ON oi.seller_id = s.seller_id
WHERE o.order_status = 'delivered'
  AND o.order_delivered_customer_date IS NOT NULL
  -- $1 and $2 act as our start and end date parameters from the CLI
  AND o.order_purchase_timestamp >= $1
  AND o.order_purchase_timestamp <= $2
GROUP BY 1, 2
HAVING total_orders > 20 -- Keeps the data statistically significant
ORDER BY avg_days_late DESC; -- Puts positive (late) numbers at the top
/*
There was some confusion with the data as no matter how many different ways it was run it would only show negative numbers
for the average days late. This is because the amount that the delivieries are late by are far outweighted by the
time it is delivered early. So for some deliveris even though the number is negative showing average deliveries on time
the majority of delivereis can still be late as long as one that was early was a lot earlier than the others were late.

*/
