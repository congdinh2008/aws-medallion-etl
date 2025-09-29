# Outputs for Meta Kaggle Pipeline

# S3 Outputs
output "s3_bucket_name" {
  description = "Tên của S3 bucket được tạo"
  value       = module.s3.bucket_name
}

output "s3_bucket_arn" {
  description = "ARN của S3 bucket"
  value       = module.s3.bucket_arn
}

output "s3_bucket_domain_name" {
  description = "Domain name của S3 bucket"
  value       = module.s3.bucket_domain_name
}

# IAM Outputs
output "glue_execution_role_arn" {
  description = "ARN của Glue execution role"
  value       = module.iam.glue_execution_role_arn
}

output "redshift_role_arn" {
  description = "ARN của Redshift service role"
  value       = module.iam.redshift_role_arn
}

output "airflow_user_access_key_id" {
  description = "Access key ID cho Airflow user"
  value       = module.iam.airflow_user_access_key_id
  sensitive   = true
}

output "airflow_user_secret_access_key" {
  description = "Secret access key cho Airflow user"
  value       = module.iam.airflow_user_secret_access_key
  sensitive   = true
}

# Glue Outputs (conditional)
output "glue_databases" {
  description = "List các Glue databases được tạo"
  value       = var.auto_deploy_advanced_services ? module.glue[0].database_names : []
}

output "glue_crawlers" {
  description = "List các Glue crawlers được tạo"
  value       = var.auto_deploy_advanced_services ? module.glue[0].crawler_names : []
}

# Redshift Serverless Outputs (conditional)
output "redshift_namespace_name" {
  description = "Tên của Redshift Serverless namespace"
  value       = var.auto_deploy_advanced_services ? module.redshift[0].namespace_name : null
}

output "redshift_workgroup_name" {
  description = "Tên của Redshift Serverless workgroup"
  value       = var.auto_deploy_advanced_services ? module.redshift[0].workgroup_name : null
}

output "redshift_workgroup_endpoint" {
  description = "Endpoint của Redshift Serverless workgroup"
  value       = var.auto_deploy_advanced_services ? module.redshift[0].workgroup_endpoint : null
  sensitive   = true
}

output "redshift_database_name" {
  description = "Tên database trong Redshift Serverless"
  value       = var.auto_deploy_advanced_services ? module.redshift[0].database_name : null
}

output "redshift_port" {
  description = "Port của Redshift Serverless"
  value       = var.auto_deploy_advanced_services ? module.redshift[0].port : null
}

output "redshift_jdbc_url" {
  description = "JDBC URL để connect tới Redshift Serverless"
  value       = var.auto_deploy_advanced_services ? module.redshift[0].jdbc_url : null
  sensitive   = true
}

# Redshift Cost Information (conditional)
output "redshift_base_capacity_rpus" {
  description = "Base capacity của Redshift Serverless"
  value       = var.auto_deploy_advanced_services ? module.redshift[0].base_capacity_rpus : null
}

output "redshift_estimated_monthly_cost" {
  description = "Estimated monthly cost range (USD) với current usage limits"
  value       = var.auto_deploy_advanced_services ? module.redshift[0].estimated_monthly_cost_range : null
}

# General Outputs
output "aws_region" {
  description = "AWS region được sử dụng"
  value       = var.aws_region
}

# Outputs for Meta Kaggle Pipeline

# Meta Kaggle Pipeline Policy Outputs
output "meta_kaggle_pipeline_policy_arn" {
  description = "ARN của Meta Kaggle Pipeline policy"
  value       = module.iam.meta_kaggle_pipeline_policy_arn
}

output "current_user_with_permissions" {
  description = "Current user được gán pipeline permissions"
  value       = module.iam.current_user_name
}

# General Environment Information
output "environment" {
  description = "Environment name"
  value       = var.environment
}

# Connection strings cho applications
output "athena_output_location" {
  description = "S3 location cho Athena query results"
  value       = "s3://${module.s3.bucket_name}/meta/athena-results/"
}

output "glue_script_location" {
  description = "S3 location cho Glue job scripts"
  value       = "s3://${module.s3.bucket_name}/scripts/glue_jobs/"
}