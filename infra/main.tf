terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

module "cur" {
  source  = "./cur.tf"
  project = var.project
}

module "glue_athena" {
  source  = "./glue_athena.tf"
  project = var.project
  cur_bucket = module.cur.cur_bucket_name
}

module "lambda" {
  source  = "./lambda.tf"
  project = var.project
  athena_db = module.glue_athena.athena_db
  athena_results_bucket = module.glue_athena.athena_results_bucket
}

module "sns" {
  source  = "./sns.tf"
  project = var.project
  email   = var.alert_email
}

