/*
This part will sort the data into ABC inventory classification. This will be based on the pareto principle to separate
the products into tiers A,B, and C, with the top 20% of revenue getting an A
the next 15 geting a B
and the rest getting a C

*/
WITH ProductRevenue AS (
    -- Find the revenue for each product, filtered by our date range parameters
    SELECT
        oi.product_id,
        SUM(oi.price) as total_revenue
    FROM order_items oi
    JOIN orders o
        ON oi.order_id = o.order_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable', 'invoiced')
      AND o.order_purchase_timestamp >= $1 -- Start date parameter
      AND o.order_purchase_timestamp <= $2 -- End date parameter
    GROUP BY 1
),
RunningTotals AS (
    -- Find a cumulative revenue for each product
    SELECT
        product_id,
        total_revenue,
        SUM(total_revenue) OVER (ORDER BY total_revenue DESC) as cumulative_revenue,
        SUM(total_revenue) OVER () as grand_total
    FROM ProductRevenue
)
-- Use when else statement to separate the different orders into their respective tiers
SELECT
    product_id,
    total_revenue,
    (cumulative_revenue / grand_total) * 100 as pct_of_total,
    CASE
        WHEN (cumulative_revenue / grand_total) <= $3 THEN 'A' -- Tier A threshold
        WHEN (cumulative_revenue / grand_total) <= $4 THEN 'B' -- Tier B threshold
        ELSE 'C'
    END as abc_tier
FROM RunningTotals
ORDER BY total_revenue DESC;
