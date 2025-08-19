module "cur" {
  source  = "../modules/cur"
  project = var.project
}

module "glue_athena" {
  source     = "../modules/glue_athena"
  project    = var.project
  cur_bucket = module.cur.cur_bucket_name
}

module "lambda" {
  source                = "../modules/lambda"
  project               = var.project
  athena_db             = module.glue_athena.athena_db
  athena_results_bucket = module.glue_athena.athena_results_bucket
}

module "sns" {
  source  = "../modules/sns"
  project = var.project
  email   = var.alert_email
}
