import argparse
from pathlib import Path

import pandas as pd

from crawler.config import CSV_OUTPUT_DIR
from crawler.database import PRODUCT_COLUMNS, initialize_database, save_products

CATEGORY_ORDER = {
    "氣炸鍋": 0,
    "冰箱": 1,
    "電視": 2,
    "掃地機器人": 3,
}


def read_csv_files(input_dir: Path) -> pd.DataFrame:
    files = sorted(input_dir.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSV files found in {input_dir}")

    frames = []
    for path in files:
        frame = pd.read_csv(path, encoding="utf-8-sig", dtype=str, keep_default_na=False)
        if len(frame.columns) != len(PRODUCT_COLUMNS):
            raise ValueError(f"{path}: expected a 12-column CSV header")
        frame.columns = PRODUCT_COLUMNS
        frames.append(frame)

    combined = pd.concat(frames, ignore_index=True)
    try:
        combined["scrape_date"] = pd.to_datetime(
            combined["scrape_date"], format="%Y-%m-%d", errors="raise"
        )
        combined["rank_position"] = pd.to_numeric(
            combined["rank_position"], errors="raise"
        ).astype(int)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"CSV contains an invalid date or rank: {exc}") from exc

    category_order = combined["category"].map(CATEGORY_ORDER)
    if category_order.isna().any():
        unknown = combined.loc[category_order.isna(), "category"].unique().tolist()
        raise ValueError(f"Unknown product category in CSV: {unknown}")
    combined["_category_order"] = category_order
    return (
        combined.sort_values(
            ["scrape_date", "_category_order", "rank_position"],
            kind="stable",
        )
        .drop(columns="_category_order")
        .reset_index(drop=True)
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import existing crawler CSV files into MySQL."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=CSV_OUTPUT_DIR,
        help="Directory containing crawler CSV files",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report import counts without inserting rows",
    )
    args = parser.parse_args()

    rows = read_csv_files(args.input_dir)
    initialize_database()
    inserted, skipped = save_products(rows, dry_run=args.dry_run)
    if args.dry_run:
        print(f"Dry run: {len(rows) - skipped} rows can be imported; {skipped} skipped.")
    else:
        print(f"Imported {inserted} rows; {skipped} existing/duplicate rows skipped.")


if __name__ == "__main__":
    main()
