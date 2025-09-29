# Variables for Meta Kaggle Pipeline Terraform Configuration

# General Configuration  
variable "aws_region" {
  description = "AWS region cho deployment"
  type        = string
  default     = "us-west-1"

  validation {
    condition     = can(regex("^[a-z]{2}-[a-z]+-[0-9]$", var.aws_region))
    error_message = "AWS region must be in valid format (e.g., us-west-1)."
  }
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

variable "project_name" {
  description = "Tên project"
  type        = string
  default     = "meta-kaggle-pipeline"
}

# Deployment Control Flags
variable "auto_deploy_advanced_services" {
  description = "Tự động deploy Glue, Athena, Redshift (cần admin permissions)"
  type        = bool
  default     = true
}

# S3 Configuration
variable "s3_bucket_name" {
  description = "Tên S3 bucket cho data lake"
  type        = string
  default     = "psi-de-glue-congdinh"
}

# Redshift Serverless Configuration
variable "redshift_database_name" {
  description = "Tên database trong Redshift Serverless"
  type        = string
  default     = "meta_warehouse"
}

variable "redshift_master_username" {
  description = "Master username cho Redshift Serverless"
  type        = string
  default     = "meta_admin"
}

variable "redshift_master_password" {
  description = "Master password cho Redshift Serverless"
  type        = string
  sensitive   = true
}

# Serverless Capacity Configuration
variable "redshift_base_capacity_rpus" {
  description = "Base capacity cho Redshift Serverless (RPUs)"
  type        = number
  default     = 8
}

variable "redshift_max_capacity_rpus" {
  description = "Maximum capacity cho Redshift Serverless (RPUs)"
  type        = number
  default     = 64
}

variable "redshift_publicly_accessible" {
  description = "Có cho phép public access không"
  type        = bool
  default     = false
}

variable "redshift_security_group_ids" {
  description = "List security group IDs cho Redshift Serverless"
  type        = list(string)
  default     = []
}

# Cost Control
variable "enable_redshift_usage_limits" {
  description = "Enable usage limits để control cost"
  type        = bool
  default     = true
}

variable "redshift_monthly_usage_limit" {
  description = "Monthly usage limit (RPU hours)"
  type        = number
  default     = 500
}

variable "redshift_weekly_usage_limit" {
  description = "Weekly usage limit (RPU hours)"
  type        = number
  default     = 125
}

variable "redshift_usage_limit_breach_action" {
  description = "Action when usage limit is breached"
  type        = string
  default     = "log"
}

# Optional Features
variable "enable_redshift_scheduled_scaling" {
  description = "Enable scheduled scaling"
  type        = bool
  default     = false
}

variable "redshift_business_hours_capacity" {
  description = "Capacity during business hours (RPUs)"
  type        = number
  default     = 32
}

variable "enable_redshift_resource_policy" {
  description = "Enable resource policy"
  type        = bool
  default     = false
}

# Network Configuration  
variable "private_subnet_ids" {
  description = "List của private subnet IDs cho Redshift Serverless (optional)"
  type        = list(string)
  default     = []
}

# Tagging
variable "common_tags" {
  description = "Common tags áp dụng cho tất cả resources"
  type        = map(string)
  default = {
    Project     = "meta-kaggle-pipeline"
    Environment = "dev"
    Owner       = "data-team"
    ManagedBy   = "terraform"
    Purpose     = "etl-pipeline"
  }
}

# Feature Flags
variable "enable_glue_development_endpoint" {
  description = "Có tạo Glue development endpoint không"
  type        = bool
  default     = false
}

variable "enable_cost_optimization" {
  description = "Enable cost optimization features"
  type        = bool
  default     = true
}

variable "enable_secrets_manager" {
  description = "Enable AWS Secrets Manager để store credentials"
  type        = bool
  default     = false # Tắt cho dev environment để tránh permission issues
}