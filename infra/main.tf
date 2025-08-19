terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

module "cur" {
  source  = "./cur"
  project = var.project
}

module "glue_athena" {
  source  = "./glue_athena"
  project = var.project
  cur_bucket = module.cur.cur_bucket_name
}

module "lambda" {
  source  = "./lambda"
  project = var.project
  athena_db = module.glue_athena.athena_db
  athena_results_bucket = module.glue_athena.athena_results_bucket
}

module "sns" {
  source  = "./sns"
  project = var.project
  email   = var.alert_email
}

