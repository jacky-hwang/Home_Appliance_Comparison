SELECT
    rank_position AS `排名`,
    product_name AS `商品名稱`,
    CAST(NULLIF(REGEXP_REPLACE(special_price, '[^0-9.]', ''), '') AS DECIMAL(12,2)) AS `價格`,
    product_url AS `網址`
FROM crawl_results
WHERE category = '{{商品種類}}'
  AND scrape_date = STR_TO_DATE('{{日期}}', '%Y/%m/%d')
ORDER BY rank_position
LIMIT 20;
