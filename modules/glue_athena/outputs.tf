output "athena_db" {
  value = aws_glue_catalog_database.athena_db.name
}

output "athena_results_bucket" {
  value = aws_s3_bucket.athena_results.id
}

