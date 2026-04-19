# Milestone 2: Python Pipeline
**Team 11**: [Alexander Zorio], [Tanzim Raffi]

## How to Run
Instructions to run the pipeline from a fresh clone:
```bash
git clone [https://github.com/](https://github.com/)[your-github-username]/wvu-ieng-331-m2-11.git
cd wvu-ieng-331-m2-11
uv sync
# place olist.duckdb in the data/ directory
uv run wvu-ieng-331-m2-11
uv run wvu-ieng-331-m2-11 --start-date 2017-01-01 --end-date 2018-01-01
```
## Parameters
| Parameter | Type | Default | Description |
|------------------------------------------|
|`--start-date` | date (str) | 2000-01-01 | The start date for filtering orders (Format: YYYY-MM-DD). |
|`--end-date` | date (str) | 2099-12-31 | The end date for filtering orders (Format: YYYY-MM-DD). |
|`--db-path` | string | data/olist.duckdb | Path to the DuckDB database file.|

## Outputs
Currently we are running outputs for one of 4 sql queries, ABC.sql. All 4 sql files can be run with few changes to the pipeline file.
There are three output files produced they are as follows:
detail.parquet: Lists individual products along with their total revenue, cumulative revenue percentage, and assigned ABC classification tier.
Contains a full scored and classified dataset.
summary.csv: Shows how many products fall into each tier and the total revenue for each tier.
chart.html: The Altair visualization, interactive bar chart, of total revenue broken into the ABC tier list.

## Validation Checks
Before running the main queries the following data checks are run. Note if a check fails a warning will be shown in the terminal with a note as to what failed
but the pipeline will run with disclaimers in the logs of problems with the data. The following checks are run:
Table Existence: Verification that all 9 expected tables are present in the dataset.
Null Checks: Verification that key columns are not completely null.
Date Range Validation: Checks orders.order_purchase_timestamp to make sure that there are no dates that exist in the future. (No time travel)
Row Count Minimums: Verifies that core tables have at least 1000 rows each.

## Analysis Summary
