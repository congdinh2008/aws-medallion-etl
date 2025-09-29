# Variables for Redshift Serverless Module

# Basic Configuration
variable "database_name" {
  description = "Tên database trong Redshift Serverless namespace"
  type        = string
}

variable "master_username" {
  description = "Master username cho Redshift Serverless"
  type        = string
}

variable "master_password" {
  description = "Master password cho Redshift Serverless"
  type        = string
  sensitive   = true
  
  validation {
    condition = (
      length(var.master_password) >= 8 &&
      length(var.master_password) <= 64 &&
      can(regex("^[A-Za-z0-9!#$%&*+,./:;=?^_`{|}~-]+$", var.master_password)) &&
      can(regex("[A-Z]", var.master_password)) &&
      can(regex("[a-z]", var.master_password)) &&
      can(regex("[0-9]", var.master_password))
    )
    error_message = "Password phải có 8-64 ký tự, có ít nhất 1 chữ hoa, 1 chữ thường, 1 số. Không được chứa /, @, \", space, \\, '"
  }
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
}

# Serverless Capacity Configuration
variable "base_capacity_rpus" {
  description = "Base capacity trong Redshift Processing Units (RPUs). Min 8 RPUs."
  type        = number
  default     = 8
  
  validation {
    condition     = var.base_capacity_rpus >= 8 && var.base_capacity_rpus <= 512
    error_message = "Base capacity phải từ 8 đến 512 RPUs."
  }
}

variable "max_capacity_rpus" {
  description = "Maximum capacity trong Redshift Processing Units (RPUs)"
  type        = number
  default     = 128
  
  validation {
    condition     = var.max_capacity_rpus >= 8 && var.max_capacity_rpus <= 512
    error_message = "Max capacity phải từ 8 đến 512 RPUs."
  }
}

# Network Configuration
variable "publicly_accessible" {
  description = "Có cho phép public access không"
  type        = bool
  default     = false
}

variable "subnet_ids" {
  description = "List of subnet IDs cho Redshift Serverless (optional)"
  type        = list(string)
  default     = null
}

variable "security_group_ids" {
  description = "List of security group IDs cho Redshift Serverless (optional)"
  type        = list(string)
  default     = null
}

# IAM Configuration
variable "redshift_role_arn" {
  description = "ARN của IAM role cho Redshift để access S3"
  type        = string
}

# Security Configuration
variable "kms_key_id" {
  description = "KMS key ID cho encryption (optional)"
  type        = string
  default     = null
}

# Usage Limits và Cost Control
variable "enable_usage_limits" {
  description = "Có enable usage limits không để control cost"
  type        = bool
  default     = true
}

variable "monthly_usage_limit_rpu_hours" {
  description = "Monthly usage limit trong RPU hours"
  type        = number
  default     = 1000 # ~$300-400 per month với $0.375/RPU-hour
}

variable "weekly_usage_limit_rpu_hours" {
  description = "Weekly usage limit trong RPU hours"  
  type        = number
  default     = 250 # 1/4 của monthly limit
}

variable "usage_limit_breach_action" {
  description = "Action khi reach usage limit: log, emit-metric, hoặc deactivate"
  type        = string
  default     = "log"
  
  validation {
    condition = contains(["log", "emit-metric", "deactivate"], var.usage_limit_breach_action)
    error_message = "Breach action phải là log, emit-metric, hoặc deactivate."
  }
}

# Scheduled Scaling (Optional)
variable "enable_scheduled_scaling" {
  description = "Có enable scheduled scaling không"
  type        = bool
  default     = false
}

variable "business_hours_capacity_rpus" {
  description = "Capacity trong business hours (8 AM - 6 PM GMT+7)"
  type        = number
  default     = 64
}

# Resource Policy
variable "enable_resource_policy" {
  description = "Có enable resource policy không"
  type        = bool
  default     = false
}

# Tags
variable "tags" {
  description = "Tags được apply cho tất cả resources"
  type        = map(string)
  default     = {}
}