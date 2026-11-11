USE tibame;

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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
