/*
We want to know who the best suppliers are within the database. We want to add up all the value of product supplied and then rank them based
on value. We also want to rank suppliers/sellers based on the number of orders they have filled. We are doing this because there may be a seller
who may have filled one large order compared to one that fills many smaller order. The supplier filling more orders may be better than the
one who only fills a couple large ones in the long run, so it seems good to look at both sides.
*/

SELECT
    s.seller_id,
    SUM(oi.price) AS total_product_value, -- Total value of each supplier's products
    COUNT(DISTINCT oi.order_id) AS total_orders_filled,
    -- Rank #1: Based on total money made
    DENSE_RANK() OVER (ORDER BY SUM(oi.price) DESC) AS rank_by_value,
    -- Rank #2: Based on total number of orders shipped
    DENSE_RANK() OVER (ORDER BY COUNT(DISTINCT oi.order_id) DESC) AS rank_by_volume
FROM sellers s
JOIN order_items oi
    ON s.seller_id = oi.seller_id
JOIN orders o
    ON oi.order_id = o.order_id
-- We strictly want orders that were successfully filled and delivered
-- $1 and $2 act as our start and end date parameters from the CLI
WHERE o.order_status = 'delivered'
  AND o.order_purchase_timestamp >= $1
  AND o.order_purchase_timestamp <= $2
GROUP BY
    s.seller_id
ORDER BY
    rank_by_value ASC
LIMIT 100;
