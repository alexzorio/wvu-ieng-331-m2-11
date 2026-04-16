-- Telling the script to use the olist file and making a shortcut to reference it !! Requires the database file to be in the same folder !!
ATTACH 'olist.duckdb' AS olist;

-- Print all the table names in the main schema
SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'main'
        AND table_name NOT IN ('current_notebook_id','has_onboarded','notebook_versions','notebooks')
    ORDER BY table_name;
/*
Show all column names, their data type, which table they belong to, and if they accept null values.
*/
SELECT table_name, column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_name NOT IN ('current_notebook_id','has_onboarded','notebook_versions','notebooks')
    ORDER BY table_name, ordinal_position;

/*
Row counts for all tables, making a new table row_counts to store all the new information.
Going through each table one by one and linking the counts together in a new table.
*/

SELECT
    'category translation' AS table_name,
    'row count' AS info,
    COUNT(*) AS value
FROM olist.category_translation
UNION ALL
SELECT
    'customers',
    'row count',
    COUNT(*)
FROM olist.customers
UNION ALL
SELECT
    'geolocation',
    'row count',
    COUNT(*)
FROM olist.geolocation
UNION ALL
SELECT
    'order items',
    'row count',
    COUNT(*)
FROM olist.order_items
UNION ALL
SELECT
    'order payments',
    'row count',
    COUNT(*)
FROM olist.order_payments
UNION ALL
SELECT
    'order reviews',
    'row count',
    COUNT(*)
FROM olist.order_reviews
UNION ALL
SELECT
    'orders',
    'row count',
    COUNT(*)
FROM olist.orders
UNION ALL
SELECT
    'products',
    'row count',
    COUNT(*)
FROM olist.products
UNION ALL
SELECT
    'sellers',
    'row count',
    COUNT(*)
FROM olist.sellers
UNION ALL

/*
Finished all the row counts, now moving onto null rates for key columns.
I am considering key columns to be ones that link information between tables. Columns that appear in multiple tables
That is why I am choosing customer_id, order_id, product_id, and seller_id
These are also the keys which if they are null or orphaned is a problem
These keys appear multiple times and I am going to document how many times they appear as null and in which table they were null
The structure of the table is going to be table name, the table we are looking in, info, which key it is and if it is primary (PK) or
foreign (FK) to the table we are looking in, and the number of nulls found in that column for value.
*/

-- First checking primary keys. A primary key is found in its own table. ex. customer_id is a primary key when found in the customer table
SELECT
    'customers' AS table_name,
    'Null PK: customer_id' AS info,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END)
FROM olist.customers
UNION ALL
SELECT
    'orders',
    'Null PK: order_id',
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END)
FROM olist.orders
UNION ALL
SELECT
    'products',
    'Null PK: product_id',
    SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END)
FROM olist.products
UNION ALL
SELECT
    'sellers',
    'Null PK: seller_id',
    SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END)
FROM olist.sellers
UNION ALL
-- Moving on to checking foreign keys being missing. Key if foreign when not in it's primary table. ex. customer_id in orders table
SELECT
    'orders' AS table_name,
    'Null FK: customer_id' AS info,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END)
FROM olist.orders
UNION ALL
SELECT
    'order_items',
    'Null FK: order_id',
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END)
FROM olist.order_items
UNION ALL
SELECT
    'order_items',
    'Null FK: product_id',
    SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END)
FROM olist.order_items
UNION ALL
SELECT
    'order_items',
    'Null FK: seller_id',
    SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END)
FROM olist.order_items
UNION ALL
SELECT
    'order_payments',
    'Null FK: order_id',
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END)
FROM olist.order_payments
UNION ALL
SELECT
    'order_reviews',
    'Null FK: order_id',
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END)
FROM olist.order_reviews
-- That is all the NULL checks for keys.

UNION ALL

/*
Now that we know the null instances of the keys I want to check if there are instances of foreign keys not matching up with primary keys. (Orphaned keys)
Logic for testing is as follows count the number of times the foreign key does not match any primary key for that key.
*/

SELECT
    'orders' AS table_name,
    'Mismatch: Invalid customer_id' AS info,
    COUNT(*) AS value
FROM olist.orders
WHERE customer_id IS NOT NULL
    AND customer_id NOT IN (
        SELECT customer_id FROM olist.customers
    )
UNION ALL
SELECT
    'order_items',
    'Mismatch: Invalid order_id',
    COUNT(*)
FROM olist.order_items
WHERE order_id IS NOT NULL
    AND order_id NOT IN (
            SELECT order_id FROM olist.orders
    )
UNION ALL
SELECT
    'order_items',
    'Mismatch: Invalid product_id',
    COUNT(*)
FROM olist.order_items
WHERE product_id IS NOT NULL
    AND product_id NOT IN (
        SELECT product_id FROM olist.products
    )
UNION ALL
SELECT
    'order_items',
    'Mismatch: Invalid seller_id',
    COUNT(*)
FROM olist.order_items
WHERE seller_id IS NOT NULL
    AND seller_id NOT IN (
        SELECT seller_id FROM olist.sellers
    )
UNION ALL
SELECT
    'order_payments',
    'Mismatch: Invalid order_id',
    COUNT(*)
FROM olist.order_payments
WHERE order_id IS NOT NULL
    AND order_id NOT IN (
        SELECT order_id FROM olist.orders
    )
UNION ALL
SELECT
    'order_reviews',
    'Mismatch: Invalid order_id',
    COUNT(*)
FROM olist.order_reviews
WHERE order_id IS NOT NULL
    AND order_id NOT IN (
        SELECT order_id FROM olist.orders
    )
-- Finish checking for orphaned keys

UNION ALL

/*
I noticed when printing out the orders table that were null instances of dates. Going to do a null count for delivery dates.
*/

SELECT
    'orders' AS table_name,
    'Null order_purchase_timestamp' AS info,
    SUM(CASE WHEN order_purchase_timestamp IS NULL THEN 1 ELSE 0 END)
FROM olist.orders
UNION ALL
SELECT
    'orders' AS table_name,
    'Null order_approved_at' AS info,
    SUM(CASE WHEN order_approved_at IS NULL THEN 1 ELSE 0 END)
FROM olist.orders
UNION ALL
SELECT
    'orders' AS table_name,
    'Null order_delivered_carrier_date' AS info,
    SUM(CASE WHEN order_delivered_carrier_date IS NULL THEN 1 ELSE 0 END)
FROM olist.orders
UNION ALL
SELECT
    'orders' AS table_name,
    'Null order_delivered_customer_date' AS info,
    SUM(CASE WHEN order_delivered_customer_date IS NULL THEN 1 ELSE 0 END)
FROM olist.orders
UNION ALL
SELECT
    'orders' AS table_name,
    'Null order_estimated_delivery_date' AS info,
    SUM(CASE WHEN order_estimated_delivery_date IS NULL THEN 1 ELSE 0 END)
FROM olist.orders
UNION ALL

/*
Moving on to checking for duplicate entries. The place we need to check for duplicates is places where data is likely to be added frequently, all the order tables.
This includes orders, order_items, order_payments, and order_reviews. The other tables likely are not updated often and are do not have harmful duplicates. What I
mean is that if a row in the customers table is duplicated if will not impact anything, it does not matter if there is a duplicate row of descriptive customer information.
Duplicate order information is harmful as two of the same order may end up getting place.
Since we checked if there were any null instances of keys previously, which there are none, we can check for duplicate keys to check for duplicate entries.
*/

SELECT
    'orders' AS table_name,
    'Duplicate Rows' AS metric,
    COUNT(*) - (SELECT COUNT(*) FROM (SELECT DISTINCT * FROM olist.orders)) AS value
FROM olist.orders

UNION ALL

SELECT
    'order_items',
    'Duplicate Rows',
    COUNT(*) - (SELECT COUNT(*) FROM (SELECT DISTINCT * FROM olist.order_items))
FROM olist.order_items

UNION ALL

SELECT
    'order_payments',
    'Duplicate Rows',
    COUNT(*) - (SELECT COUNT(*) FROM (SELECT DISTINCT * FROM olist.order_payments))
FROM olist.order_payments

UNION ALL

SELECT
    'order_reviews',
    'Duplicate Rows',
    COUNT(*) - (SELECT COUNT(*) FROM (SELECT DISTINCT * FROM olist.order_reviews))
FROM olist.order_reviews

ORDER BY info DESC, table_name;


/*
Moving on to look at the date range for the database. Needs to be its own table because of data types. Choosing purchase date to represent time span of data
because entries are dependent on something being purchased.
*/

SELECT
    'orders' AS table_name,
    'first order placed' AS info,
    MIN(order_purchase_timestamp) AS date_info
FROM olist.ORDERS
UNION ALL
SELECT
    'orders' AS table_name,
    'last order placed' AS info,
    MAX(order_purchase_timestamp) AS date_info
FROM olist.ORDERS;
