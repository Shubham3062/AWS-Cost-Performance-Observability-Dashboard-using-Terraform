variable "project" {
  description = "Project name prefix"
  type        = string
  default     = "aws-cost-dashboard"
}

variable "alert_email" {
  description = "Email address for cost alerts"
  type        = string
  default     = "shahshubham130@gmail.com"
}
