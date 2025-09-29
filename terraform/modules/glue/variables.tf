# Variables for Glue Module

variable "glue_role_arn" {
  description = "ARN của Glue execution role"
  type        = string
}

variable "s3_bucket_name" {
  description = "Tên S3 bucket cho Glue scripts và data"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "tags" {
  description = "Tags áp dụng cho Glue resources"
  type        = map(string)
  default     = {}
}