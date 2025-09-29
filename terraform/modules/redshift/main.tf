# Meta Kaggle Pipeline - Redshift Serverless Module
# Sử dụng Redshift Serverless để tối ưu cost và loại bỏ infrastructure complexity

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Data sources
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# Local values
locals {
  namespace_name = "meta-${var.environment}-namespace"
  workgroup_name = "meta-${var.environment}-workgroup"
}

# ================================
# REDSHIFT SERVERLESS NAMESPACE
# ================================
# Namespace định nghĩa database và user credentials

resource "aws_redshiftserverless_namespace" "meta_namespace" {
  namespace_name = local.namespace_name
  db_name        = var.database_name
  
  # Admin credentials
  admin_username      = var.master_username
  admin_user_password = var.master_password
  
  # IAM roles cho data access
  iam_roles = [var.redshift_role_arn]
  
  # Logging configuration
  log_exports = ["userlog", "connectionlog", "useractivitylog"]
  
  # KMS encryption (optional)
  kms_key_id = var.kms_key_id
  
  tags = merge(var.tags, {
    Name    = "Meta Redshift Serverless Namespace"
    Purpose = "serverless-namespace"
  })
}

# ================================
# REDSHIFT SERVERLESS WORKGROUP  
# ================================
# Workgroup định nghĩa compute capacity và configuration

resource "aws_redshiftserverless_workgroup" "meta_workgroup" {
  namespace_name = aws_redshiftserverless_namespace.meta_namespace.namespace_name
  workgroup_name = local.workgroup_name
  
  # Capacity configuration (in RPUs - Redshift Processing Units)
  base_capacity = var.base_capacity_rpus
  max_capacity  = var.max_capacity_rpus
  
  # Network configuration
  publicly_accessible = var.publicly_accessible
  
  # Only set VPC configuration if subnet_ids are provided
  subnet_ids         = length(var.subnet_ids) > 0 ? var.subnet_ids : null
  security_group_ids = length(var.security_group_ids) > 0 ? var.security_group_ids : null
  
  # Enhanced VPC routing (only if using VPC)
  enhanced_vpc_routing = length(var.subnet_ids) > 0 ? true : false
  
  tags = merge(var.tags, {
    Name         = "Meta Redshift Serverless Workgroup"
    Purpose      = "serverless-compute"
    BaseCapacity = "${var.base_capacity_rpus}RPUs"
    MaxCapacity  = "${var.max_capacity_rpus}RPUs"
  })
  
  depends_on = [aws_redshiftserverless_namespace.meta_namespace]
}

# ================================
# USAGE LIMITS (Cost Control)
# ================================
# Thiết lập usage limits để control cost

resource "aws_redshiftserverless_usage_limit" "monthly_limit" {
  count = var.enable_usage_limits ? 1 : 0
  
  resource_arn = aws_redshiftserverless_workgroup.meta_workgroup.arn
  usage_type   = "serverless-compute"
  amount       = var.monthly_usage_limit_rpu_hours
  period       = "monthly"
  
  # Action khi reach limit
  breach_action = var.usage_limit_breach_action # "log", "emit-metric", hoặc "deactivate"
  
  depends_on = [aws_redshiftserverless_workgroup.meta_workgroup]
}

# Weekly limit cho fine-grained control
resource "aws_redshiftserverless_usage_limit" "weekly_limit" {
  count = var.enable_usage_limits ? 1 : 0
  
  resource_arn = aws_redshiftserverless_workgroup.meta_workgroup.arn
  usage_type   = "serverless-compute"
  amount       = var.weekly_usage_limit_rpu_hours
  period       = "weekly"
  
  breach_action = var.usage_limit_breach_action
  
  depends_on = [aws_redshiftserverless_workgroup.meta_workgroup]
}
