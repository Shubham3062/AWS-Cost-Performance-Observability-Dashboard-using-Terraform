terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "eu-west-2"
}

module "cur" {
  source  = "./"
  project = var.project
}

module "glue_athena" {
  source  = "./"
  project = var.project
  cur_bucket = module.cur.cur_bucket_name
}

module "lambda" {
  source  = "./"
  project = var.project
  athena_db = module.glue_athena.athena_db
  athena_results_bucket = module.glue_athena.athena_results_bucket
}

module "sns" {
  source  = "./"
  project = var.project
  email   = var.alert_email
}

