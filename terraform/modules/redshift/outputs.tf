# Outputs for Redshift Serverless Module

# Namespace Outputs
output "namespace_name" {
  description = "Tên của Redshift Serverless namespace"
  value       = aws_redshiftserverless_namespace.meta_namespace.namespace_name
}

output "namespace_arn" {
  description = "ARN của Redshift Serverless namespace"
  value       = aws_redshiftserverless_namespace.meta_namespace.arn
}

output "namespace_id" {
  description = "ID của Redshift Serverless namespace"
  value       = aws_redshiftserverless_namespace.meta_namespace.namespace_id
}

# Workgroup Outputs
output "workgroup_name" {
  description = "Tên của Redshift Serverless workgroup"
  value       = aws_redshiftserverless_workgroup.meta_workgroup.workgroup_name
}

output "workgroup_arn" {
  description = "ARN của Redshift Serverless workgroup"
  value       = aws_redshiftserverless_workgroup.meta_workgroup.arn
}

output "workgroup_id" {
  description = "ID của Redshift Serverless workgroup"
  value       = aws_redshiftserverless_workgroup.meta_workgroup.workgroup_id
}

output "workgroup_endpoint" {
  description = "Endpoint của Redshift Serverless workgroup"
  value       = aws_redshiftserverless_workgroup.meta_workgroup.endpoint
  sensitive   = true
}

# Database Connection Information
output "database_name" {
  description = "Tên database trong Redshift Serverless"
  value       = var.database_name
}

output "port" {
  description = "Port để connect tới Redshift Serverless (default: 5439)"
  value       = 5439
}

# Connection URLs
output "jdbc_url" {
  description = "JDBC URL để connect tới Redshift Serverless"
  value       = "jdbc:redshift://${aws_redshiftserverless_workgroup.meta_workgroup.endpoint[0].address}:${aws_redshiftserverless_workgroup.meta_workgroup.endpoint[0].port}/${var.database_name}"
  sensitive   = true
}

# Configuration Information
output "base_capacity_rpus" {
  description = "Base capacity của Serverless workgroup trong RPUs"
  value       = var.base_capacity_rpus
}

output "max_capacity_rpus" {
  description = "Maximum capacity của Serverless workgroup trong RPUs"
  value       = var.max_capacity_rpus
}

# Usage Limits Information
output "usage_limits_enabled" {
  description = "Có enable usage limits không"
  value       = var.enable_usage_limits
}

output "monthly_usage_limit" {
  description = "Monthly usage limit trong RPU hours"
  value       = var.enable_usage_limits ? var.monthly_usage_limit_rpu_hours : null
}

output "weekly_usage_limit" {
  description = "Weekly usage limit trong RPU hours"
  value       = var.enable_usage_limits ? var.weekly_usage_limit_rpu_hours : null
}

# Cost Information
output "estimated_monthly_cost_range" {
  description = "Estimated monthly cost range (USD) based on usage limits"
  value = var.enable_usage_limits ? {
    minimum = var.monthly_usage_limit_rpu_hours * 0.25  # $0.25/RPU-hour là giá thấp nhất
    maximum = var.monthly_usage_limit_rpu_hours * 0.45  # $0.45/RPU-hour là giá cao nhất
  } : null
}