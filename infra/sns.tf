resource "aws_sns_topic" "cost_dashboard_topic" {
  name = "${var.project}-sns"
}

resource "aws_sns_topic_subscription" "email_sub" {
  topic_arn = aws_sns_topic.cost_dashboard_topic.arn
  protocol  = "email"
  endpoint  = "shahshubham130@gmail.com"
}

