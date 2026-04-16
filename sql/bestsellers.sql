/*
We want to know who the best suppliers are within the database. We want to add up all the value of product supplied and then rank them based
on value. We also want to rank suppliers/sellers based on the number of orders they have filled. We are doing this because there may be a seller
who may have filled one large order compared to one that fills many smaller order. The supplier filling more orders may be better than the
one who only fills a couple large ones in the long run, so it seems good to look at both sides.
*/

SELECT
    s.seller_id,
    SUM(oi.price) AS total_product_value, -- Total value of each suppliers product
    COUNT(DISTINCT oi.order_id) AS total_orders_filled,
    -- Rank #1: Based on total money made
    DENSE_RANK() OVER (ORDER BY SUM(oi.price) DESC) AS rank_by_value,
    -- Rank #2: Based on total number of orders shipped
    DENSE_RANK() OVER (ORDER BY COUNT(DISTINCT oi.order_id) DESC) AS rank_by_volume
FROM olist.sellers s -- Joining three tables (sellers, order_items, orders)
JOIN olist.order_items oi
    ON s.seller_id = oi.seller_id
JOIN olist.orders o
    ON oi.order_id = o.order_id
-- We strictly want orders that were successfully filled and delivered
WHERE o.order_status = 'delivered'
GROUP BY
    s.seller_id
ORDER BY
    rank_by_value ASC -- For now ranking based on value but we could change to ORDER BY rank_by_volume to look at rankings based on the number of orders filled
LIMIT 100;
