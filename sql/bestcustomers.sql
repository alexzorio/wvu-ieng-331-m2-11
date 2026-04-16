/*
We want to know who the best customers are within the database. First we want to add up all the spending of all the customers and then rank them based
on spending. We also want to rank customers based on the number of orders they place.
*/

-- Requires the olist.duckdb file to be in the same folder to run
ATTACH 'olist.duckdb' AS olist;

SELECT
    c.customer_unique_id, -- We are selecting customer_unique_id instead of customer_id because customer_id is a temp key made for each entry.
    SUM(op.payment_value) AS total_spent,
    COUNT(DISTINCT o.order_id) AS total_orders,
    DENSE_RANK() OVER (ORDER BY SUM(op.payment_value) DESC) AS customer_rank
FROM olist.customers c -- Joining customers and orders
JOIN olist.orders o
    ON c.customer_id = o.customer_id
JOIN olist.order_payments op -- Joining order_payments and orders
    ON o.order_id = op.order_id
-- We only want to count orders that weren't canceled, unavailable, or invoiced. Only counting orders we know are completed.
WHERE o.order_status NOT IN ('canceled', 'unavailable','invoiced')
GROUP BY
    c.customer_unique_id
ORDER BY
    customer_rank ASC
LIMIT 100;
