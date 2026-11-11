SELECT
    `商品種類`,
    `資料日期`,
    `排名`,
    `商品名稱`,
    `製造商`,
    `價格`,
    `評分`,
    `評論數`,
    `網址`
FROM (
    SELECT
        cr.category AS `商品種類`,
        DATE_FORMAT(cr.scrape_date, '%Y/%m/%d') AS `資料日期`,
        cr.rank_position AS `排名`,
        cr.product_name AS `商品名稱`,
        cr.manufacturer AS `製造商`,
        CAST(NULLIF(REGEXP_REPLACE(cr.special_price, '[^0-9.]', ''), '') AS DECIMAL(12,2)) AS `價格`,
        CAST(NULLIF(TRIM(CAST(cr.rating AS CHAR)), '') AS DECIMAL(3,1)) AS `評分`,
        CAST(NULLIF(REGEXP_REPLACE(CAST(cr.review_count AS CHAR), '[^0-9]', ''), '') AS UNSIGNED) AS `評論數`,
        cr.product_url AS `網址`
    FROM crawl_results AS cr
    INNER JOIN (
        SELECT category, MAX(DATE(scrape_date)) AS latest_scrape_date
        FROM crawl_results
        GROUP BY category
    ) AS latest
        ON cr.category = latest.category
       AND DATE(cr.scrape_date) = latest.latest_scrape_date
) AS product_stats
WHERE `評分` >= 4.5
  AND `評論數` >= 20
ORDER BY `商品種類` ASC, `排名` ASC;
