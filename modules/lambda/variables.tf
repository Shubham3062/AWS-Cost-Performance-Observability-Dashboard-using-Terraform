variable "project" {
  description = "Project name prefix"
  type        = string
}

variable "athena_db" {
  description = "Athena database name"
  type        = string
}

variable "athena_results_bucket" {
  description = "S3 bucket for Athena query results"
  type        = string
}

