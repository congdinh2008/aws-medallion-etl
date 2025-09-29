# Outputs for Glue Module

# Database Outputs
output "bronze_database_name" {
  description = "Tên của Bronze database"
  value       = aws_glue_catalog_database.bronze_database.name
}

output "silver_database_name" {
  description = "Tên của Silver database"
  value       = aws_glue_catalog_database.silver_database.name
}

output "gold_database_name" {
  description = "Tên của Gold database"
  value       = aws_glue_catalog_database.gold_database.name
}

output "database_names" {
  description = "List tất cả database names"
  value = [
    aws_glue_catalog_database.bronze_database.name,
    aws_glue_catalog_database.silver_database.name,
    aws_glue_catalog_database.gold_database.name
  ]
}

# Crawler Outputs
output "bronze_crawler_names" {
  description = "List tên của Bronze crawlers"
  value       = [for k, v in aws_glue_crawler.bronze_table_crawlers : v.name]
}

output "silver_crawler_names" {
  description = "List tên của Silver crawlers"
  value       = [for k, v in aws_glue_crawler.silver_table_crawlers : v.name]
}

output "gold_dimension_crawler_names" {
  description = "List tên của Gold dimension crawlers"
  value       = [for k, v in aws_glue_crawler.gold_dimension_crawlers : v.name]
}

output "gold_fact_crawler_names" {
  description = "List tên của Gold fact crawlers"
  value       = [for k, v in aws_glue_crawler.gold_fact_crawlers : v.name]
}

output "crawler_names" {
  description = "List tất cả crawler names"
  value = concat(
    [for k, v in aws_glue_crawler.bronze_table_crawlers : v.name],
    [for k, v in aws_glue_crawler.silver_table_crawlers : v.name],
    [for k, v in aws_glue_crawler.gold_dimension_crawlers : v.name],
    [for k, v in aws_glue_crawler.gold_fact_crawlers : v.name]
  )
}

# Job Outputs
output "bronze_job_names" {
  description = "List tên của Bronze Glue jobs"
  value       = [for k, v in aws_glue_job.bronze_transformation_jobs : v.name]
}

output "silver_job_names" {
  description = "List tên của Silver Glue jobs"
  value       = [for k, v in aws_glue_job.silver_transformation_jobs : v.name]
}

output "gold_dimension_job_names" {
  description = "List tên của Gold dimension jobs"
  value       = [for k, v in aws_glue_job.gold_dimension_jobs : v.name]
}

output "gold_fact_job_names" {
  description = "List tên của Gold fact jobs"
  value       = [for k, v in aws_glue_job.gold_fact_jobs : v.name]
}

output "all_job_names" {
  description = "List tất cả Glue job names"
  value = concat(
    [for k, v in aws_glue_job.bronze_transformation_jobs : v.name],
    [for k, v in aws_glue_job.silver_transformation_jobs : v.name], 
    [for k, v in aws_glue_job.gold_dimension_jobs : v.name],
    [for k, v in aws_glue_job.gold_fact_jobs : v.name]
  )
}

# Athena Outputs
output "athena_workgroup_name" {
  description = "Tên của Athena workgroup"
  value       = aws_athena_workgroup.meta_analytics.name
}

output "athena_workgroup_arn" {
  description = "ARN của Athena workgroup"
  value       = aws_athena_workgroup.meta_analytics.arn
}