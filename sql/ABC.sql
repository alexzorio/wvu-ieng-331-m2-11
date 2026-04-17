/*
This part will sort the data into ABC inventory classification. This will be based on the pareto principle to separate
the products into tiers A,B, and C, with the top 20% of revenue getting an A
the next 15 geting a B
and the rest getting a C

*/
ATTACH 'olist.duckdb' AS olist;

WITH ProductRevenue AS (
    -- Find the revenue for each product
    SELECT
        product_id,
        SUM(price) as total_revenue
    FROM olist.order_items
    GROUP BY 1
),
RunningTotals AS (
    -- Find a cumulative revenue for each product
    SELECT
        product_id,
        total_revenue,
        SUM(total_revenue) OVER (ORDER BY total_revenue DESC) as cumulative_revenue, -- Cumulative revenue for each product
        SUM(total_revenue) OVER () as grand_total -- Grand total revenue for all products
    FROM ProductRevenue
)
-- Use when else statement to separate the different orders into their respective tiers by revenue percentage
SELECT
    product_id,
    total_revenue,
    (cumulative_revenue / grand_total) * 100 as pct_of_total, -- Percentage of total revenue for each product
    CASE
        WHEN (cumulative_revenue / grand_total) <= 0.80 THEN 'A' -- Top 20% of revenue gets an A
        WHEN (cumulative_revenue / grand_total) <= 0.95 THEN 'B' -- Next 15% gets a B
        ELSE 'C' -- The rest gets a C
    END as abc_tier
FROM RunningTotals
ORDER BY total_revenue DESC; -- Order by total revenue descending
