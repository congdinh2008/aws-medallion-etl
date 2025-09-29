# Outputs for S3 Module

output "bucket_name" {
  description = "Tên của S3 bucket"
  value       = aws_s3_bucket.meta_data_lake.bucket
}

output "bucket_arn" {
  description = "ARN của S3 bucket"
  value       = aws_s3_bucket.meta_data_lake.arn
}

output "bucket_domain_name" {
  description = "Domain name của S3 bucket"
  value       = aws_s3_bucket.meta_data_lake.bucket_domain_name
}

output "bucket_regional_domain_name" {
  description = "Regional domain name của S3 bucket"
  value       = aws_s3_bucket.meta_data_lake.bucket_regional_domain_name
}

output "bucket_id" {
  description = "ID của S3 bucket"
  value       = aws_s3_bucket.meta_data_lake.id
}

# Data paths for applications
output "raw_data_path" {
  description = "S3 path cho raw data"
  value       = "s3://${aws_s3_bucket.meta_data_lake.bucket}/meta/raw/"
}

output "bronze_data_path" {
  description = "S3 path cho bronze data"
  value       = "s3://${aws_s3_bucket.meta_data_lake.bucket}/meta/bronze/"
}

output "silver_data_path" {
  description = "S3 path cho silver data"
  value       = "s3://${aws_s3_bucket.meta_data_lake.bucket}/meta/silver/"
}

output "gold_data_path" {
  description = "S3 path cho gold data"
  value       = "s3://${aws_s3_bucket.meta_data_lake.bucket}/meta/gold/"
}

output "athena_results_path" {
  description = "S3 path cho Athena query results"
  value       = "s3://${aws_s3_bucket.meta_data_lake.bucket}/meta/athena-results/"
}

output "glue_scripts_path" {
  description = "S3 path cho Glue job scripts"
  value       = "s3://${aws_s3_bucket.meta_data_lake.bucket}/scripts/glue_jobs/"
}