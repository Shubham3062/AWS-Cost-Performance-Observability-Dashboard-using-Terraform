resource "aws_s3_bucket" "cur_bucket" {
  bucket = "${var.project}-cur-bucket"
  force_destroy = true
}

resource "aws_cur_report_definition" "cur" {
  report_name         = "cost-report"
  time_unit           = "DAILY"
  format              = "Parquet"
  compression         = "Parquet"
  additional_schema_elements = ["RESOURCES"]
  s3_bucket           = aws_s3_bucket.cur_bucket.bucket
  s3_region           = "eu-west-2"
  s3_prefix           = "cur"
  report_versioning   = "CREATE_NEW_REPORT"
}

resource "aws_s3_bucket_policy" "cur_policy" {
  bucket = aws_s3_bucket.cur_bucket.id
  policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowCURDelivery",
      "Effect": "Allow",
      "Principal": { "Service": "billingreports.amazonaws.com" },
      "Action": "s3:*",
      "Resource": [
        "arn:aws:s3:::${aws_s3_bucket.cur_bucket.bucket}",
        "arn:aws:s3:::${aws_s3_bucket.cur_bucket.bucket}/*"
      ]
    }
  ]
}
POLICY
}
