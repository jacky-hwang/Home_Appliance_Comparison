SELECT
    rank_position AS `排名`,
    product_name AS `商品名稱`,
    manufacturer AS `製造商`,
    CAST(NULLIF(REGEXP_REPLACE(special_price, '[^0-9.]', ''), '') AS DECIMAL(12,2)) AS `價格`,
    rating AS `評分`,
    review_count AS `評論數`,
    features AS `商品特色`,
    product_id AS `商品編號`,
    product_url AS `網址`
FROM crawl_results
WHERE category = '{{商品種類}}'
  AND scrape_date = STR_TO_DATE('{{日期}}', '%Y/%m/%d')
ORDER BY rank_position
LIMIT 20;
