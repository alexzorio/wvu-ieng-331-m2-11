import argparse
from pathlib import Path
from typing import Optional, Sequence

import altair as alt
import duckdb
import polars as pl
from loguru import logger

from wvu_ieng_331_m2_11.queries import get_abc_analysis

# Import the modules
from wvu_ieng_331_m2_11.validation import validate_database


def parse_args(args: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """
    Parses command-line arguments for the pipeline.

    Args:
        args (Optional[Sequence[str]]): Command-line arguments. Defaults to sys.argv.

    Returns:
        argparse.Namespace: Parsed arguments containing start_date, end_date, and db_path.
    """
    parser = argparse.ArgumentParser(description="Olist E-commerce Data Pipeline")

    # We use dates far in the past/future as defaults so running with no arguments
    # produces the full, unfiltered analysis
    parser.add_argument(
        "--start-date",
        type=str,
        default="2000-01-01",
        help="Start date for analysis (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default="2099-12-31",
        help="End date for analysis (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default="data/olist.duckdb",
        help="Path to the DuckDB database file",
    )

    return parser.parse_args(args)


def main(args: Optional[Sequence[str]] = None) -> None:
    """
    Main entry point for the data pipeline. Directs validation,
    querying, processing, and output generation.

    Args:
        args (Optional[Sequence[str]]): Command-line arguments.

    Raises:
        FileNotFoundError: If the database file is missing.
        duckdb.Error: If a database operation fails.
        ValueError: If dates or parameters are invalid.
        OSError: If output files cannot be written.
    """
    parsed_args = parse_args(args)
    db_path = Path(parsed_args.db_path)

    # 1. Create output directory if it doesn't exist
    output_dir = Path("output")
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(
        f"Starting pipeline with start_date={parsed_args.start_date}, end_date={parsed_args.end_date}"
    )

    try:
        # 2. Validation Layer
        logger.info("Running validation...")
        is_valid = validate_database(db_path)
        if not is_valid:
            logger.warning(
                "Validation flagged warnings. Proceeding with pipeline anyway."
            )

        # 3. Query Layer: Get the full scored dataset (ABC Analysis)
        logger.info("Executing queries...")
        df_detail = get_abc_analysis(
            db_path=db_path,
            start_date=parsed_args.start_date,
            end_date=parsed_args.end_date,
        )

        # 4. Process Layer: Create aggregated summary metrics
        logger.info("Processing data...")
        df_summary = (
            df_detail.group_by("abc_tier")
            .agg(
                [
                    pl.col("product_id").count().alias("total_products"),
                    pl.col("total_revenue").sum().round(2).alias("tier_revenue"),
                ]
            )
            .sort("abc_tier")
        )

        # 5. Output Layer: Generate the 3 required files
        logger.info("Generating output files...")

        # Output 1: detail.parquet (The full scored/classified dataset)
        detail_path = output_dir / "detail.parquet"
        df_detail.write_parquet(detail_path)
        logger.info(f"Saved detail data to {detail_path}")

        # Output 2: summary.csv (Aggregated metrics)
        summary_path = output_dir / "summary.csv"
        df_summary.write_csv(summary_path)
        logger.info(f"Saved summary data to {summary_path}")

        # Output 3: chart.html (Altair Visualization)
        chart_path = output_dir / "chart.html"

        # Build a bar chart showing Revenue by ABC Tier
        chart = (
            alt.Chart(df_summary.to_pandas())
            .mark_bar()
            .encode(
                x=alt.X("abc_tier:N", title="ABC Tier"),
                y=alt.Y("tier_revenue:Q", title="Total Revenue ($)"),
                color=alt.Color("abc_tier:N", legend=None),
                tooltip=["abc_tier", "total_products", "tier_revenue"],
            )
            .properties(
                title="Total Revenue by ABC Classification Tier", width=400, height=300
            )
        )

        chart.save(str(chart_path))
        logger.info(f"Saved chart visualization to {chart_path}")

        logger.info("Pipeline completed successfully!")

    except FileNotFoundError as e:
        logger.error(f"File not found error: {e}")
        raise
    except duckdb.Error as e:
        logger.error(f"Database error during pipeline execution: {e}")
        raise
    except ValueError as e:
        logger.error(f"Parameter error: {e}")
        raise
    except OSError as e:
        logger.error(f"System I/O error writing output files: {e}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        raise


if __name__ == "__main__":
    main()
