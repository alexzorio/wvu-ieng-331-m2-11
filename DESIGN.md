# Design Rationale

## Parameter Flow

When the user runs the pipeline, command-line arguments (such as `--start-date 2017-01-01`) are first intercepted by the `parse_args()` function inside `pipeline.py`. This function uses Python's built-in `argparse` module to map the command-line inputs to a `Namespace` object (e.g., `parsed_args.start_date`).

From there, `pipeline.py` passes these variables into the data access layer by calling `get_abc_analysis(db_path, start_date=parsed_args.start_date, end_date=parsed_args.end_date)`.

Inside `queries.py`, the `get_abc_analysis` function receives these dates. It reads the raw SQL string from the file, establishes a connection to DuckDB, and passes the Python date strings directly into DuckDB's `.execute()` method using the `parameters=[start_date, end_date]` argument. DuckDB then places these values into the SQL query where the `$1` and `$2` placeholders to set the proper timeframe for analysis.

## SQL Parameterization

In this pipeline, I used the `ABC.sql` file as the primary query.
Raw SQL: Inside `ABC.sql`, the `WHERE` clause filters dates using placeholders instead of hardcoded strings: `WHERE o.order_purchase_timestamp >= $1::TIMESTAMP AND o.order_purchase_timestamp <= $2::TIMESTAMP`.

How file is read: `queries.py` uses `Path(__file__).resolve().parent.parent.parent / "sql" / "ABC.sql"` to locate the file, reads it as a string using `.read_text()`, and relies on DuckDB's parameterized execution (`execute(query, parameters=[...])`) to swap `$1` and `$2` with the actual Python date variables.

Why Parameterized: Parameterized queries are better than Python f-strings for SQL. F-strings leave the database vulnerable to SQL injection attacks and can cause syntax crashes if a string contains something unexpected. Parameterization forces the database engine to treat the inputs strictly as values, not actual code.

Why .sql files: Keeping SQL in its own `.sql` files rather than inline Python strings keeps the file tree clean, tidy, and makes writing queries much easier. It allows for proper syntax highlighting in IDEs, makes the queries easier to test directly in a SQL, and keeps the Python logic clean and readable later.

## Validation Logic

For each validation check in `validation.py`, explain:
- What it checks and why that check matters
- What happens if it fails (halt vs. warning)
- How you chose your thresholds (e.g., why 1,000 rows as a minimum)

## Error Handling

Pick 2 specific `try/except` blocks in your code and explain:
- What exception you catch and why that specific type
- What the code does when the exception is raised
- What would happen to the user if you used a bare `except:` instead

## Scaling & Adaptation

Answer both:
1. If the Olist dataset grew to 10 million orders, what part of your pipeline would break or slow down first? What would you change?
2. If you needed to add a third output format (e.g., a JSON API response), where in your code would you add it and what functions would you modify?
