from datetime import datetime
from pathlib import Path
from typing import Union

import duckdb
from loguru import logger


def validate_database(db_path: Union[str, Path]) -> bool:
    """
    Runs data validation checks before running the pipeline.

    Args:
        db_path (Union[str, Path]): Path to the DuckDB database file.

    Returns:
        bool: True if all checks pass, False if there are warnings.

    Raises:
        FileNotFoundError: If the database file does not exist.
        duckdb.Error: If a database query fails.
    """
    if not Path(db_path).exists():
        raise FileNotFoundError(f"Database file not found: {db_path}")

    logger.info("Starting validation...")
    passed = True

    try:
        # Connect to the database
        con = duckdb.connect(str(db_path))

        # --- Check 1: Do all 9 expected tables exist? ---
        tables_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        # fetchall() returns a list of tuples like [('orders',), ('customers',)]
        result = con.execute(tables_query).fetchall()

        # Convert the list of tuples into a simple list of strings
        found_tables = [row[0] for row in result]

        expected_tables = [
            "customers",
            "geolocation",
            "order_items",
            "order_payments",
            "order_reviews",
            "orders",
            "products",
            "sellers",
            "product_category_name_translation",
        ]

        for table in expected_tables:
            if table not in found_tables:
                logger.warning(
                    f"Validation Warning: Table '{table}' is missing from the database."
                )
                passed = False

        # --- Check 2: Are key columns entirely NULL? ---
        # We write out the exact queries to avoid any SQL string concatenation
        null_queries = [
            "SELECT COUNT(order_id) FROM orders",
            "SELECT COUNT(customer_id) FROM customers",
            "SELECT COUNT(product_id) FROM order_items",
            "SELECT COUNT(seller_id) FROM order_items",
        ]

        for query in null_queries:
            count = con.execute(query).fetchone()[0]
            if count == 0:
                logger.warning(
                    f"Validation Warning: A key column is completely NULL. Query: {query}"
                )
                passed = False

        # --- Check 3: Is the date range valid? ---
        date_query = "SELECT MIN(order_purchase_timestamp), MAX(order_purchase_timestamp) FROM orders"
        min_date, max_date = con.execute(date_query).fetchone()

        if min_date is None or max_date is None:
            logger.warning("Validation Warning: order_purchase_timestamp has no dates.")
            passed = False
        elif max_date > datetime.now():
            logger.warning(
                f"Validation Warning: order_purchase_timestamp contains future dates ({max_date})."
            )
            passed = False

        # --- Check 4: Do core tables have at least 1,000 rows? ---
        row_queries = [
            "SELECT COUNT(*) FROM orders",
            "SELECT COUNT(*) FROM order_items",
            "SELECT COUNT(*) FROM customers",
        ]

        for query in row_queries:
            count = con.execute(query).fetchone()[0]
            if count < 1000:
                logger.warning(
                    f"Validation Warning: Table has less than 1000 rows. Query: {query}"
                )
                passed = False

        # Close the connection
        con.close()

    except duckdb.Error as e:
        logger.error(f"Database error during validation: {e}")
        raise duckdb.Error(f"Database error: {e}")

    if passed:
        logger.info("Validation completed perfectly!")
    else:
        logger.warning("Validation finished with warnings. See logs above.")

    return passed
