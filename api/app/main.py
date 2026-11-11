import logging
from datetime import date
from typing import Annotated, Any

from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import bindparam, text
from sqlalchemy.exc import SQLAlchemyError

from app.config import engine

app = FastAPI(
    title="PChome 家電比價 API",
    description="查詢 PChome 家電商品目前價格、歷史價格與價格變動。",
    version="1.0.0",
)
logger = logging.getLogger(__name__)


def _record_values(row: Any) -> dict[str, Any]:
    numeric_fields = {
        "price",
        "original_price_amount",
        "previous_price",
        "price_change",
        "price_change_percent",
    }
    return {
        key: float(value) if key in numeric_fields and value is not None else value
        for key, value in row.items()
    }


PRICE_EXPRESSION = (
    "CAST(NULLIF(REGEXP_REPLACE({column}, '[^0-9.]', ''), '') AS DECIMAL(12, 2))"
)
LATEST_PRODUCTS = f"""
    WITH ranked_products AS (
        SELECT id, scrape_date, rank_position, category, product_name, manufacturer,
               special_price, original_price, rating, review_count, features,
               product_url, product_id,
               ROW_NUMBER() OVER (
                   PARTITION BY product_id
                   ORDER BY scrape_date DESC, rank_position ASC, id DESC
               ) AS product_row_number
        FROM crawl_results
    ),
    latest_products AS (
        SELECT *, {PRICE_EXPRESSION.format(column="special_price")} AS price,
                  {PRICE_EXPRESSION.format(column="original_price")} AS original_price_amount
        FROM ranked_products
        WHERE product_row_number = 1
    )
"""


def _query(sql: str, parameters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    try:
        with engine.connect() as connection:
            return [
                _record_values(row)
                for row in connection.execute(text(sql), parameters or {})
                .mappings()
                .all()
            ]
    except SQLAlchemyError as exc:
        logger.exception("Crawler database query failed")
        raise HTTPException(
            status_code=503,
            detail="Crawler database is unavailable",
        ) from exc


@app.get("/health")
def health() -> dict[str, str]:
    _query("SELECT 1 AS status")
    return {"status": "ok"}


@app.get("/categories")
def list_categories() -> list[dict[str, Any]]:
    return _query(
        """
        SELECT category, MAX(scrape_date) AS latest_scrape_date,
               COUNT(DISTINCT product_id) AS tracked_products
        FROM crawl_results
        GROUP BY category
        ORDER BY category
        """
    )


@app.get("/products")
def list_products(
    category: Annotated[str | None, Query(max_length=32)] = None,
    keyword: Annotated[str | None, Query(max_length=200)] = None,
    min_price: Annotated[float | None, Query(ge=0)] = None,
    max_price: Annotated[float | None, Query(ge=0)] = None,
    sort_by: Annotated[
        str, Query(pattern="^(price_asc|price_desc|rank|newest)$")
    ] = "price_asc",
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> dict[str, Any]:
    if min_price is not None and max_price is not None and min_price > max_price:
        raise HTTPException(
            status_code=422, detail="min_price must be less than or equal to max_price"
        )

    filters: list[str] = []
    parameters: dict[str, Any] = {"limit": limit, "offset": offset}
    for column, parameter, value, operator in (
        ("category", "category", category, "="),
        ("price", "min_price", min_price, ">="),
        ("price", "max_price", max_price, "<="),
    ):
        if value is not None:
            filters.append(f"{column} {operator} :{parameter}")
            parameters[parameter] = value
    if keyword:
        filters.append(
            "(product_name LIKE :keyword OR manufacturer LIKE :keyword "
            "OR product_id LIKE :keyword)"
        )
        parameters["keyword"] = f"%{keyword}%"
    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    ordering = {
        "price_asc": "price IS NULL, price ASC, product_id",
        "price_desc": "price IS NULL, price DESC, product_id",
        "rank": "rank_position ASC, product_id",
        "newest": "scrape_date DESC, rank_position ASC, product_id",
    }[sort_by]

    count_sql = text(f"{LATEST_PRODUCTS} SELECT COUNT(*) AS total FROM latest_products {where_clause}")
    products_sql = text(
        f"""
        {LATEST_PRODUCTS}
        SELECT id, product_id, scrape_date, rank_position, category, product_name,
               manufacturer, special_price, price, original_price,
               original_price_amount, rating, review_count, features, product_url
        FROM latest_products
        {where_clause}
        ORDER BY {ordering}
        LIMIT :limit OFFSET :offset
        """
    )
    try:
        with engine.connect() as connection:
            total = connection.execute(count_sql, parameters).scalar_one()
            rows = (
                connection.execute(products_sql, parameters)
                .mappings()
                .all()
            )
    except SQLAlchemyError as exc:
        logger.exception("Crawler product query failed")
        raise HTTPException(status_code=503, detail="Crawler database is unavailable") from exc
    return {
        "items": [_record_values(row) for row in rows],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@app.get("/products/compare")
def compare_products(
    product_ids: Annotated[list[str], Query(min_length=2, max_length=20)],
) -> dict[str, Any]:
    product_ids = list(dict.fromkeys(product_ids))
    if len(product_ids) < 2:
        raise HTTPException(status_code=422, detail="Select at least two unique products")

    statement = text(
        f"""
        {LATEST_PRODUCTS}
        SELECT product_id, scrape_date, rank_position, category, product_name,
               manufacturer, special_price, price, original_price,
               original_price_amount, rating, review_count, product_url
        FROM latest_products
        WHERE product_id IN :product_ids
        """
    ).bindparams(bindparam("product_ids", expanding=True))
    rows = _query_expanding(statement, {"product_ids": product_ids})
    found_ids = {row["product_id"] for row in rows}
    missing_ids = [product_id for product_id in product_ids if product_id not in found_ids]
    if missing_ids:
        raise HTTPException(
            status_code=404,
            detail={"message": "Some products were not found", "product_ids": missing_ids},
        )

    positions = {product_id: index for index, product_id in enumerate(product_ids)}
    products = sorted(rows, key=lambda row: positions[row["product_id"]])
    prices = [float(row["price"]) for row in products if row["price"] is not None]
    lowest = min(prices) if prices else None
    highest = max(prices) if prices else None
    difference = highest - lowest if lowest is not None and highest is not None else None
    return {
        "products": products,
        "price_comparison": {
            "lowest_price": lowest,
            "highest_price": highest,
            "difference": difference,
            "difference_percent_from_lowest": (
                round(difference / lowest * 100, 2)
                if difference is not None and lowest
                else None
            ),
        },
    }


def _query_expanding(statement: Any, parameters: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        with engine.connect() as connection:
            return [
                _record_values(row)
                for row in connection.execute(statement, parameters).mappings().all()
            ]
    except SQLAlchemyError as exc:
        logger.exception("Crawler product comparison query failed")
        raise HTTPException(status_code=503, detail="Crawler database is unavailable") from exc


@app.get("/products/{product_id}/history")
def product_price_history(product_id: str) -> dict[str, Any]:
    rows = _query(
        f"""
        WITH prices AS (
            SELECT scrape_date, rank_position, category, product_name, manufacturer,
                   special_price, {PRICE_EXPRESSION.format(column="special_price")} AS price,
                   original_price,
                   {PRICE_EXPRESSION.format(column="original_price")} AS original_price_amount,
                   rating, review_count, product_url, product_id
            FROM crawl_results
            WHERE product_id = :product_id
        ),
        history AS (
            SELECT *, LAG(price) OVER (ORDER BY scrape_date, rank_position) AS previous_price
            FROM prices
        )
        SELECT scrape_date, rank_position, category, product_name, manufacturer,
               special_price, price, original_price, original_price_amount,
               rating, review_count, product_url, previous_price,
               price - previous_price AS price_change,
               CASE
                   WHEN previous_price IS NOT NULL AND previous_price <> 0
                   THEN ROUND((price - previous_price) / previous_price * 100, 2)
                   ELSE NULL
               END AS price_change_percent
        FROM history
        ORDER BY scrape_date
        """,
        {"product_id": product_id},
    )
    if not rows:
        raise HTTPException(status_code=404, detail="Product price history not found")
    return {"product_id": product_id, "count": len(rows), "history": rows}


@app.get("/price-changes")
def list_price_changes(
    category: Annotated[str | None, Query(max_length=32)] = None,
    direction: Annotated[str, Query(pattern="^(up|down|all)$")] = "all",
    start_date: date | None = None,
    end_date: date | None = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> dict[str, Any]:
    if start_date and end_date and start_date > end_date:
        raise HTTPException(
            status_code=422, detail="start_date must be on or before end_date"
        )

    conditions = ["previous_price IS NOT NULL", "price IS NOT NULL", "price <> previous_price"]
    parameters: dict[str, Any] = {"limit": limit, "offset": offset}
    if category is not None:
        conditions.append("category = :category")
        parameters["category"] = category
    if start_date is not None:
        conditions.append("scrape_date >= :start_date")
        parameters["start_date"] = start_date
    if end_date is not None:
        conditions.append("scrape_date <= :end_date")
        parameters["end_date"] = end_date
    if direction == "up":
        conditions.append("price > previous_price")
    elif direction == "down":
        conditions.append("price < previous_price")

    changes = f"""
        WITH prices AS (
            SELECT scrape_date, rank_position, category, product_name, manufacturer,
                   product_url, product_id,
                   {PRICE_EXPRESSION.format(column="special_price")} AS price
            FROM crawl_results
        ),
        history AS (
            SELECT *, LAG(price) OVER (
                PARTITION BY product_id ORDER BY scrape_date, rank_position
            ) AS previous_price
            FROM prices
        ),
        changes AS (
            SELECT *, price - previous_price AS price_change,
                   ROUND((price - previous_price) / NULLIF(previous_price, 0) * 100, 2)
                       AS price_change_percent
            FROM history
        )
    """
    where_clause = " AND ".join(conditions)
    try:
        with engine.connect() as connection:
            total = connection.execute(
                text(f"{changes} SELECT COUNT(*) FROM changes WHERE {where_clause}"),
                parameters,
            ).scalar_one()
            rows = (
                connection.execute(
                    text(
                        f"""
                        {changes}
                        SELECT scrape_date, category, product_id, product_name,
                               manufacturer, previous_price, price, price_change,
                               price_change_percent, product_url
                        FROM changes
                        WHERE {where_clause}
                        ORDER BY scrape_date DESC, ABS(price_change) DESC, product_id
                        LIMIT :limit OFFSET :offset
                        """
                    ),
                    parameters,
                )
                .mappings()
                .all()
            )
    except SQLAlchemyError as exc:
        logger.exception("Crawler price changes query failed")
        raise HTTPException(status_code=503, detail="Crawler database is unavailable") from exc
    return {
        "items": [_record_values(row) for row in rows],
        "total": total,
        "limit": limit,
        "offset": offset,
    }
