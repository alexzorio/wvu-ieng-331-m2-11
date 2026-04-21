# Milestone 2 Grade

**Team 11**

| Category | Score | Max |
|---|---|---|
| Pipeline Functionality | 6 | 6 |
| Parameterization | 6 | 6 |
| Code Quality | 6 | 6 |
| Project Structure | 3 | 3 |
| DESIGN.md | 3 | 3 |
| **Total** | **24** | **24** |

---

## Pipeline Functionality — 6/6

Pipeline runs cleanly on the standard and extended (holdout) databases without errors. All three required outputs are produced on every run: `detail.parquet`, `summary.csv`, and `chart.html`. Date parameters correctly filter the query results. Validation layer runs before queries and gracefully handles a missing `product_category_name_translation` table with a warning rather than a crash.

## Parameterization — 6/6

Three CLI parameters implemented via argparse: `--start-date`, `--end-date`, and `--db-path`. Parameters are passed through to the SQL layer using DuckDB positional placeholders (`$1`, `$2`). Validation is real: `get_abc_analysis` raises `ValueError` if `start_date > end_date` or if `tier_a >= tier_b`; `validate_database` raises `FileNotFoundError` if the DB path does not exist. Additional internal parameters `tier_a` and `tier_b` (ABC threshold fractions) are validated with clear error messages.

## Code Quality — 6/6

- **Type hints**: consistent use of `Union[str, Path]`, `Optional[Sequence[str]]`, and return types throughout all modules.
- **Docstrings**: all public functions have complete docstrings with Args, Returns, and Raises sections.
- **loguru**: used for INFO, WARNING, and ERROR-level messages across pipeline, queries, and validation modules.
- **pathlib**: `Path` used everywhere for file and directory handling.
- **Specific exceptions**: `FileNotFoundError`, `duckdb.Error`, `ValueError`, and `OSError` are caught and re-raised with context. No bare `except` clauses.
- Minor note: `__init__.py` contains a stale no-op `main()` stub that is not wired to the entry point, but this does not affect functionality.

## Project Structure — 3/3

Clean `src/` layout with a properly configured `pyproject.toml` entry point (`wvu_ieng_331_m2_11.pipeline:main`). SQL queries are externalized to a dedicated `sql/` directory (4 files: ABC.sql, bestcustomers.sql, bestsellers.sql, DeliveryTime.sql). README.md and DESIGN.md are present. `uv sync` and `uv run` work out of the box.

## DESIGN.md — 3/3

Five well-developed sections: Parameter Flow, SQL Parameterization, Validation Logic, Error Handling, and Scaling & Adaptation. Each section references specific functions, file paths, and code constructs rather than generic descriptions. Rationale is provided for design choices (why parameterized queries over f-strings, why `.sql` files, why non-halting validation, why specific exception types). Scaling section identifies the actual bottleneck (`df_summary.to_pandas()` for large datasets) with a concrete mitigation strategy.
