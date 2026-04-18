from pathlib import Path
from typing import Union

import duckdb
import polars as pl

# Define the base directory for your SQL files.
# Adjust the .parent chain depending on where queries.py lives relative to the sql/ folder.
# Example: If queries.py is in src/my_package/ and sql/ is at the repo root, use .parent.parent
SQL_DIR = Path(__file__).resolve().parent.parent / "sql"


def get_abc_analysis(
    db_path: Union[str, Path],
    start_date: str,
    end_date: str,
    tier_a: float = 0.80,
    tier_b: float = 0.95,
) -> pl.DataFrame:
    """
    Executes the ABC analysis query against DuckDB and returns a Polars DataFrame.

    Args:
        db_path (Union[str, Path]): Path to the DuckDB database file.
        start_date (str): The start date for filtering (YYYY-MM-DD).
        end_date (str): The end date for filtering (YYYY-MM-DD).
        tier_a (float): Cumulative revenue threshold for Tier A (default: 0.80).
        tier_b (float): Cumulative revenue threshold for Tier B (default: 0.95).

    Returns:
        pl.DataFrame: A Polars DataFrame containing the ABC analysis results.

    Raises:
        FileNotFoundError: If the database file or SQL file does not exist.
        ValueError: If tier_a is greater than or equal to tier_b, or dates are invalid.
        OSError: If there is a system-level issue reading the SQL file.
        duckdb.Error: If the SQL query fails to execute properly.
    """
    if tier_a >= tier_b:
        raise ValueError("tier_a threshold must be strictly less than tier_b.")
    if start_date > end_date:
        raise ValueError("start_date cannot be after end_date.")

    db_path_obj = Path(db_path)
    if not db_path_obj.exists():
        raise FileNotFoundError(f"Database file not found: {db_path_obj}")

    sql_file = SQL_DIR / "ABC.sql"

    try:
        with open(sql_file, "r", encoding="utf-8") as f:
            query = f.read()
    except FileNotFoundError:
        raise FileNotFoundError(f"SQL file not found at: {sql_file}")
    except OSError as e:
        raise OSError(f"Failed to read SQL file {sql_file}: {e}")

    try:
        with duckdb.connect(str(db_path_obj)) as con:
            # Passes parameters as a list. DuckDB injects them into $1, $2, $3, $4
            return con.execute(query, [start_date, end_date, tier_a, tier_b]).pl()
    except duckdb.Error as e:
        raise duckdb.Error(f"Failed to execute ABC analysis query: {e}")


def get_best_customers(
    db_path: Union[str, Path], start_date: str, end_date: str
) -> pl.DataFrame:
    """
    Executes the best customers query against DuckDB and returns a Polars DataFrame.

    Args:
        db_path (Union[str, Path]): Path to the DuckDB database file.
        start_date (str): The start date for filtering (YYYY-MM-DD).
        end_date (str): The end date for filtering (YYYY-MM-DD).

    Returns:
        pl.DataFrame: A Polars DataFrame containing the top customers.

    Raises:
        FileNotFoundError: If the database file or SQL file does not exist.
        ValueError: If start_date is after end_date.
        OSError: If there is a system-level issue reading the SQL file.
        duckdb.Error: If the SQL query fails to execute properly.
    """
    if start_date > end_date:
        raise ValueError("start_date cannot be after end_date.")

    db_path_obj = Path(db_path)
    if not db_path_obj.exists():
        raise FileNotFoundError(f"Database file not found: {db_path_obj}")

    sql_file = SQL_DIR / "bestcustomers.sql"

    try:
        with open(sql_file, "r", encoding="utf-8") as f:
            query = f.read()
    except FileNotFoundError:
        raise FileNotFoundError(f"SQL file not found at: {sql_file}")
    except OSError as e:
        raise OSError(f"Failed to read SQL file {sql_file}: {e}")

    try:
        with duckdb.connect(str(db_path_obj)) as con:
            return con.execute(query, [start_date, end_date]).pl()
    except duckdb.Error as e:
        raise duckdb.Error(f"Failed to execute best customers query: {e}")
