SELECT
    line_item_usage_start_date AS date,
    SUM(line_item_unblended_cost) AS ec2_cost,
    SUM(line_item_usage_amount) AS ec2_hours
FROM cur_cur_daily_parquet
WHERE product_product_name = 'Amazon Elastic Compute Cloud'
GROUP BY line_item_usage_start_date
ORDER BY date ASC;
