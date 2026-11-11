from collections.abc import Iterable, Sequence
from datetime import date, datetime
from typing import Any

import pandas as pd
from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.engine import Connection, Engine, URL

from crawler.config import (
    MYSQL_DATABASE,
    MYSQL_HOST,
    MYSQL_PASSWORD,
    MYSQL_PORT,
    MYSQL_USER,
)

DATABASE_URL = URL.create(
    "mysql+pymysql",
    username=MYSQL_USER,
    password=MYSQL_PASSWORD,
    host=MYSQL_HOST,
    port=MYSQL_PORT,
    database=MYSQL_DATABASE,
)
engine: Engine = create_engine(DATABASE_URL, pool_pre_ping=True)

CREATE_CRAWL_RESULTS_TABLE = """
CREATE TABLE IF NOT EXISTS crawl_results (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    scrape_date DATE NOT NULL,
    rank_position SMALLINT UNSIGNED NOT NULL,
    category VARCHAR(32) NOT NULL,
    product_name TEXT NOT NULL,
    manufacturer TEXT NOT NULL,
    special_price VARCHAR(100) NOT NULL,
    original_price VARCHAR(100) NOT NULL,
    rating VARCHAR(50) NOT NULL,
    review_count VARCHAR(50) NOT NULL,
    features TEXT NOT NULL,
    product_url VARCHAR(512) NOT NULL,
    product_id VARCHAR(64) NOT NULL,
    PRIMARY KEY (id),
    INDEX idx_category_scrape_date (category, scrape_date),
    INDEX idx_product_id (product_id),
    UNIQUE KEY uq_crawl_results_date_category_rank (scrape_date, category, rank_position)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

PRODUCT_COLUMNS = (
    "scrape_date",
    "rank_position",
    "category",
    "product_name",
    "manufacturer",
    "special_price",
    "original_price",
    "rating",
    "review_count",
    "features",
    "product_url",
    "product_id",
)


def initialize_database() -> None:
    with engine.begin() as connection:
        connection.execute(text(CREATE_CRAWL_RESULTS_TABLE))
        column_result = connection.execute(
            text(
                """
                SELECT COLUMN_NAME
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = :database AND TABLE_NAME = 'crawl_results'
                """
            ),
            {"database": MYSQL_DATABASE},
        )
        columns = {row[0] for row in column_result}
        index_result = connection.execute(
            text(
                """
                SELECT INDEX_NAME
                FROM information_schema.STATISTICS
                WHERE TABLE_SCHEMA = :database AND TABLE_NAME = 'crawl_results'
                """
            ),
            {"database": MYSQL_DATABASE},
        )
        indexes = {row[0] for row in index_result}

        alterations = []
        if "idx_category_scraped_at" in indexes:
            alterations.append("DROP INDEX idx_category_scraped_at")
        for column in ("run_id", "scraped_at"):
            if column in columns:
                alterations.append(f"DROP COLUMN {column}")
        if "idx_category_scrape_date" not in indexes:
            alterations.append(
                "ADD INDEX idx_category_scrape_date (category, scrape_date)"
            )
        if "uq_crawl_results_date_category_rank" not in indexes:
            duplicate = connection.execute(
                text(
                    """
                    SELECT scrape_date, category, rank_position, COUNT(*)
                    FROM crawl_results
                    GROUP BY scrape_date, category, rank_position
                    HAVING COUNT(*) > 1
                    LIMIT 1
                    """
                )
            ).first()
            if duplicate:
                raise ValueError(
                    "Cannot add unique key: duplicate crawl result for "
                    f"date={duplicate[0]}, category={duplicate[1]}, "
                    f"rank={duplicate[2]} ({duplicate[3]} rows)"
                )
            alterations.append(
                "ADD UNIQUE INDEX uq_crawl_results_date_category_rank "
                "(scrape_date, category, rank_position)"
            )
        if alterations:
            connection.execute(
                text(f"ALTER TABLE crawl_results {', '.join(alterations)}")
            )


def _product_values(row: Sequence[str | int]) -> dict[str, date | int | str]:
    return {
        "scrape_date": datetime.strptime(str(row[0]), "%Y-%m-%d").date(),
        "rank_position": int(row[1]),
        "category": str(row[2]),
        "product_name": str(row[3]),
        "manufacturer": str(row[4]),
        "special_price": str(row[5]),
        "original_price": str(row[6]),
        "rating": str(row[7]),
        "review_count": str(row[8]),
        "features": str(row[9]),
        "product_url": str(row[10]),
        "product_id": str(row[11]),
    }


def _upsert_dataframe(
    table: pd.io.sql.SQLTable,
    connection: Connection,
    keys: list[str],
    data_iter: Iterable[tuple[Any, ...]],
) -> int:
    rows = [dict(zip(keys, row)) for row in data_iter]
    if not rows:
        return 0

    statement = mysql_insert(table.table).values(rows)
    update_values = {
        column: statement.inserted[column]
        for column in PRODUCT_COLUMNS[3:]
    }
    result = connection.execute(
        statement.on_duplicate_key_update(**update_values)
    )
    return result.rowcount


def _upload_dataframe(frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    frame.loc[:, list(PRODUCT_COLUMNS)].to_sql(
        "crawl_results",
        con=engine,
        if_exists="append",
        index=False,
        method=_upsert_dataframe,
    )


def save_product(row: Sequence[str | int]) -> None:
    if len(row) != 12:
        raise ValueError(f"Expected 12 product fields, got {len(row)}")

    frame = pd.DataFrame([_product_values(row)], columns=PRODUCT_COLUMNS)
    _upload_dataframe(frame)

    logger.info(
        "Saved {} product {} to MySQL",
        row[2],
        row[11],
    )


def save_products(
    rows: pd.DataFrame | Sequence[Sequence[str | int]], *, dry_run: bool = False
) -> tuple[int, int]:
    if isinstance(rows, pd.DataFrame):
        frame = rows.loc[:, PRODUCT_COLUMNS].copy()
    else:
        if not rows:
            return 0, 0
        prepared_rows = []
        for row in rows:
            if len(row) != 12:
                raise ValueError(f"Expected 12 product fields, got {len(row)}")
            try:
                prepared_rows.append(_product_values(row))
            except ValueError as exc:
                raise ValueError(f"Invalid date or rank in product row: {row!r}") from exc
        frame = pd.DataFrame(prepared_rows, columns=PRODUCT_COLUMNS)

    if frame.empty:
        return 0, 0

    try:
        frame["scrape_date"] = pd.to_datetime(
            frame["scrape_date"], format="%Y-%m-%d", errors="raise"
        ).dt.date
        frame["rank_position"] = pd.to_numeric(
            frame["rank_position"], errors="raise"
        ).astype(int)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"CSV contains an invalid date or rank: {exc}") from exc

    frame = frame.drop_duplicates(
        subset=["scrape_date", "category", "rank_position"], keep="first"
    )
    start_date = frame["scrape_date"].min()
    end_date = frame["scrape_date"].max()
    with engine.begin() as connection:
        result = connection.execute(
            text(
                """
                SELECT scrape_date, category, rank_position
                FROM crawl_results
                WHERE scrape_date BETWEEN :start_date AND :end_date
                """
            ),
            {"start_date": start_date, "end_date": end_date},
        )
        existing_keys = {(row[0], row[1], row[2]) for row in result.fetchall()}
    keys = list(zip(frame["scrape_date"], frame["category"], frame["rank_position"]))
    frame = frame.loc[[key not in existing_keys for key in keys]]
    rows_to_insert = len(frame)
    if rows_to_insert and not dry_run:
        _upload_dataframe(frame)

    inserted = rows_to_insert if not dry_run else 0
    skipped = len(rows) - rows_to_insert
    logger.info(
        "{} {} product rows; skipped {} existing/duplicate rows",
        "Would import" if dry_run else "Imported",
        rows_to_insert,
        skipped,
    )
    return inserted, skipped
