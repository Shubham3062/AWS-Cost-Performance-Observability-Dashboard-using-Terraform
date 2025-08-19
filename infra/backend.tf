terraform {
  backend "s3" {
    bucket         = "my-terraform-project-backend-bucket"
    key            = "state/terraform.tfstate"
    region         = "eu-west-2"
    dynamodb_table = "terraform-locks"
    encrypt        = true
  }
}

