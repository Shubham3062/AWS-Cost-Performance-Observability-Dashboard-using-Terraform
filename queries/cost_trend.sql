SELECT
    line_item_usage_start_date AS date,
    SUM(line_item_unblended_cost) AS total_cost
FROM cur_cur_daily_parquet
WHERE line_item_unblended_cost > 0
GROUP BY line_item_usage_start_date
ORDER BY date ASC;
