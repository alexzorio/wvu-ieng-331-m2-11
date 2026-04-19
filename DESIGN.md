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

The `validation.py` module runs four checks using the `validate_database` function:
1. Table Existence Check: Queries `information_schema.tables` to ensure all 9 Olist tables are present. Why it matters: A missing table will crash the pipeline's SQL joins.
2. Null Checks: Uses `COUNT(column)` to verify that key columns (`order_id`, `customer_id`, etc.) aren't completely empty. Why it matters: Primary and foreign keys are essential; if they are null, relational joins will not work correctly or at all.
3. Date Range Check: Queries the `MAX(order_purchase_timestamp)` to ensure the database doesn't contain dates in the future. Why it matters: Future dates indicate data corruption, time travel, or errors in data entry.
4. Row Count Thresholds: Verifies core tables have at least 1,000 rows. Why it matters: It ensures we are not analyzing an empty or small test table. I chose 1,000 because it is small enough to pass subset testing, but large enough to prove the table isn't just a handful of random rows thrown in to make the table not empty.

Failure Action: If any check fails, the pipeline uses `loguru` to issue a `WARNING` in the terminal, but it returns `False` and does not halt the script. This allows the pipeline to process potentially incomplete databases rather than stopping the process or crashing. It is very apparent in the terminal if you are running an incomplete database through the script.

## Error Handling

I utilized specific exception types in `pipeline.py` to handle expected failure points:
1. `except FileNotFoundError as e:` - What it catches: Triggered if the user inputs a `--db-path` that doesn't actually lead to an actual file.
   - What the code does: It uses `loguru` to log an error stating that the file is missing, then raises the error to stop the script.
2. `except duckdb.Error as e:` - What it catches: Triggers if duckDB encounters a SQL syntax error, missing table, or mismatched schema during a query.
   - What the code does: It logs the specific database error so the user knows it was a database issue and not a Python logic issue.

Why not a bare `except:`: Using a bare `except:` catches everything that can go wrong. This makes the script very difficult to stop when unexpected errors occur and does not help to identify the cause of the error. This makes debugging the issues much more difficult than it needs to be.

## Scaling & Adaptation

1. **Scaling:** If the dataset grew to 10 million orders, the part of my code that would crash first is the creation of the Altair chart. Right now, the pipeline converts the data into a Pandas dataframe to then draw the graph (`df_summary.to_pandas()`). If we tried to do that with 10 million rows, it would probably eat up all of most people's computer memory and freeze. To fix this, I would make sure to do all the computation and table summaries inside the duckDB SQL query first. That way, Python only has to graph a pre-calculated summary table instead of trying to load millions of individual rows into memory.

2. **Adaptation (JSON Output):** If I needed to save a new output format like a JSON file, I would need to change the `pipeline.py` script. Specifically, I would go to the very bottom where the output files are generated and add code that would look like: `df_summary.write_json(output_dir / "summary.json")`. I would not have to change the `queries.py` or `validation.py` files. The project was built output last in first place so adding a new way to save the data doesn't require any changes to how the data is queried or validated.
