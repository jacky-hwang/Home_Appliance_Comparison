USE tibame;

ALTER TABLE crawl_results
    ADD UNIQUE KEY uq_crawl_results_date_category_rank (
        scrape_date,
        category,
        rank_position
    );
