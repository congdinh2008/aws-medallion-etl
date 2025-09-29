# Variables for S3 Module

variable "bucket_name" {
  description = "Tên của S3 bucket"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "region" {
  description = "AWS region"
  type        = string
}

variable "enable_cost_optimization" {
  description = "Enable cost optimization features như intelligent tiering"
  type        = bool
  default     = true
}

variable "tags" {
  description = "Tags áp dụng cho S3 resources"
  type        = map(string)
  default     = {}
}