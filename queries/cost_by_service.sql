SELECT
    line_item_usage_start_date,
    product_product_name AS service,
    SUM(line_item_unblended_cost) AS cost
FROM cur_cur_daily_parquet
WHERE line_item_unblended_cost > 0
GROUP BY line_item_usage_start_date, product_product_name
ORDER BY line_item_usage_start_date ASC, cost DESC
