terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

module "cur" {
  source  = "../modules/cur"

}

module "glue_athena" {
  source  = "../modules/glue_athena"
}

module "lambda" {
  source  = "../modules/lambda"
}

module "sns" {
  source  = "../modules/sns"
}

