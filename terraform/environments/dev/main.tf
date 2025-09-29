# Meta Kaggle Pipeline - Terraform Main Configuration
# Environment: Dev
# Region: us-west-1

terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Backend configuration cho Terraform state  
  # Comment out backend cho lần init đầu tiên
  # Uncomment sau khi tạo S3 bucket và DynamoDB table

  # backend "s3" {
  #   bucket         = "psi-de-terraform-state"
  #   key            = "meta-pipeline/dev/terraform.tfstate"
  #   region         = "us-west-1"
  #   encrypt        = true
  #   dynamodb_table = "terraform-state-locks"
  # }
}

# Provider configuration
provider "aws" {
  region = var.aws_region

  default_tags {
    tags = var.common_tags
  }
}

# Data sources
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# S3 Module - Data Lake Storage
module "s3" {
  source = "../../modules/s3"

  bucket_name = var.s3_bucket_name
  environment = var.environment
  region      = var.aws_region

  tags = var.common_tags
}

# IAM Module - Roles và Policies
module "iam" {
  source = "../../modules/iam"

  s3_bucket_name         = var.s3_bucket_name
  s3_bucket_arn          = module.s3.bucket_arn
  environment            = var.environment
  enable_secrets_manager = var.enable_secrets_manager

  tags = var.common_tags
}

# Glue Module - Data Catalog và Jobs (Conditional)
module "glue" {
  count  = var.auto_deploy_advanced_services ? 1 : 0
  source = "../../modules/glue"

  glue_role_arn  = module.iam.glue_execution_role_arn
  s3_bucket_name = var.s3_bucket_name
  environment    = var.environment

  # Dependencies
  depends_on = [module.iam]

  tags = var.common_tags
}

# Redshift Serverless Module - Data Warehouse (Conditional)
module "redshift" {
  count  = var.auto_deploy_advanced_services ? 1 : 0
  source = "../../modules/redshift"

  database_name   = var.redshift_database_name
  master_username = var.redshift_master_username
  master_password = var.redshift_master_password

  # Serverless configuration
  base_capacity_rpus = var.redshift_base_capacity_rpus
  max_capacity_rpus  = var.redshift_max_capacity_rpus

  # Network configuration (optional for Serverless)
  publicly_accessible = var.redshift_publicly_accessible
  subnet_ids          = var.private_subnet_ids
  security_group_ids  = var.redshift_security_group_ids

  # IAM role
  redshift_role_arn = module.iam.redshift_role_arn

  # Cost control
  enable_usage_limits           = var.enable_redshift_usage_limits
  monthly_usage_limit_rpu_hours = var.redshift_monthly_usage_limit
  weekly_usage_limit_rpu_hours  = var.redshift_weekly_usage_limit
  usage_limit_breach_action     = var.redshift_usage_limit_breach_action

  # Optional features
  enable_scheduled_scaling     = var.enable_redshift_scheduled_scaling
  business_hours_capacity_rpus = var.redshift_business_hours_capacity
  enable_resource_policy       = var.enable_redshift_resource_policy

  environment = var.environment

  # Dependencies
  depends_on = [module.iam]

  tags = var.common_tags
}