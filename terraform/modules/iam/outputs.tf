# Outputs for IAM Module

# Meta Kaggle Pipeline Policy Outputs
output "meta_kaggle_pipeline_policy_arn" {
  description = "ARN của Meta Kaggle Pipeline policy"
  value       = aws_iam_policy.meta_kaggle_pipeline_policy.arn
}

output "meta_kaggle_pipeline_policy_name" {
  description = "Tên của Meta Kaggle Pipeline policy"
  value       = aws_iam_policy.meta_kaggle_pipeline_policy.name
}

output "current_user_name" {
  description = "Current user được gán permissions"
  value       = local.current_username
}

# Glue Role Outputs
output "glue_execution_role_arn" {
  description = "ARN của Glue execution role"
  value       = aws_iam_role.glue_execution_role.arn
}

output "glue_execution_role_name" {
  description = "Tên của Glue execution role"
  value       = aws_iam_role.glue_execution_role.name
}

# Redshift Role Outputs
output "redshift_role_arn" {
  description = "ARN của Redshift service role"
  value       = aws_iam_role.redshift_service_role.arn
}

output "redshift_role_name" {
  description = "Tên của Redshift service role"
  value       = aws_iam_role.redshift_service_role.name
}

# Airflow User Outputs
output "airflow_user_name" {
  description = "Tên của Airflow IAM user"
  value       = aws_iam_user.airflow_user.name
}

output "airflow_user_arn" {
  description = "ARN của Airflow IAM user"
  value       = aws_iam_user.airflow_user.arn
}

output "airflow_user_access_key_id" {
  description = "Access key ID cho Airflow user"
  value       = aws_iam_access_key.airflow_access_key.id
  sensitive   = true
}

output "airflow_user_secret_access_key" {
  description = "Secret access key cho Airflow user"
  value       = aws_iam_access_key.airflow_access_key.secret
  sensitive   = true
}

# Secrets Manager Outputs
output "airflow_credentials_secret_arn" {
  description = "ARN của Airflow credentials secret"
  value       = var.enable_secrets_manager ? aws_secretsmanager_secret.airflow_credentials[0].arn : null
}

output "airflow_credentials_secret_name" {
  description = "Tên của Airflow credentials secret"
  value       = var.enable_secrets_manager ? aws_secretsmanager_secret.airflow_credentials[0].name : null
}