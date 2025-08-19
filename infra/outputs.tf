output "cur_bucket" {
  value = module.cur.cur_bucket_name
}

output "athena_db" {
  value = module.glue_athena.athena_db
}

output "lambda_function" {
  value = module.lambda.lambda_name
}

output "sns_topic" {
  value = module.sns.sns_topic_arn
}
