# Milestone 2: Python Pipeline
**Team 11**: [Alexander Zorio], [Tanzim Raffi]

## How to Run
Instructions to run the pipeline from a fresh clone:
```bash
git clone [https://github.com/alexzorio/wvu-ieng-331-m2-11.git](https://github.com/alexzorio/wvu-ieng-331-m2-11.git)
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
The ABC analysis shows a distribution of data much like a pareto chart. The majority of value for orders falls within the A category which is relatively small
number of products compared to their total value. This suggests that a small portion of the product catalogue drives the majority of sales. This leads to a
recommendation for the company to maybe remove some of the B category products and possibly most of C as they may not generate enough revenue to justify the
cost of production when the A category products sell so well in comparison.

## Limitation & Caveats
One current limitation is the current hardcoded tier threshold for the ABC tier lists. This could changed into a dynamic CLI argument on top of date filtering.
Another limitation is the current process of having to change code in the pipeline to adapt to the other prepared sql queries in the sql folder. All of these
files have been parameterized to be used in the pipeline but the pipeline itself is not currently able to run them without small changes.
Additionally the current pipeline is assuming that the Schema will not be changed. If there are updates to column or table names the sql queries and validation
checks will require manual updates.
