# Variables for IAM Module

variable "s3_bucket_name" {
  description = "Tên của S3 bucket để grant permissions"
  type        = string
}

variable "s3_bucket_arn" {
  description = "ARN của S3 bucket để grant permissions"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "enable_secrets_manager" {
  description = "Enable AWS Secrets Manager để store credentials"
  type        = bool
  default     = false
}

variable "tags" {
  description = "Tags áp dụng cho IAM resources"
  type        = map(string)
  default     = {}
}