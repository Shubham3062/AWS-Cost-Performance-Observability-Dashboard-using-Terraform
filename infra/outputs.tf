output "cur_bucket" {
  value = aws_s3_bucket.cur_bucket.bucket
}

output "glue_db" {
  value = aws_glue_catalog_database.cur_db.name
}

output "athena_query_results" {
  value = aws_s3_bucket.athena_results.bucket
}

output "visualization_bucket" {
  value = aws_s3_bucket.visualization_bucket.bucket
}

output "sns_topic" {
  value = aws_sns_topic.cost_dashboard_topic.arn
}
