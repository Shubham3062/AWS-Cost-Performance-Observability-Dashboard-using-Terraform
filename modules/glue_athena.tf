resource "aws_glue_catalog_database" "cur_db" {
  name = "${var.project}_cur_db"
}

resource "aws_glue_crawler" "cur_crawler" {
  name          = "${var.project}-crawler"
  database_name = aws_glue_catalog_database.cur_db.name

  s3_target {
    path = "s3://${aws_s3_bucket.cur_bucket.bucket}/cur/"
  }

  schedule = "cron(0 12 * * ? *)"
}

resource "aws_s3_bucket" "athena_results" {
  bucket        = "${var.project}-athena-results"
  force_destroy = true
}
