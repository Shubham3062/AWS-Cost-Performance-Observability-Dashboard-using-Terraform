resource "aws_s3_bucket" "visualization_bucket" {
  bucket        = "${var.project}-visualizations"
  force_destroy = true
}

resource "aws_lambda_function" "cost_summary" {
  function_name = "${var.project}-lambda"
  filename      = "${path.module}/../lambda/cost_summary.zip"
  handler       = "cost_summary.lambda_handler"
  runtime       = "python3.9"
}

resource "aws_cloudwatch_event_rule" "daily_trigger" {
  name                = "${var.project}-daily"
  schedule_expression = "cron(0 8 * * ? *)"
}

resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.daily_trigger.name
  target_id = "lambda"
  arn       = aws_lambda_function.cost_summary.arn
}

