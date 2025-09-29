# IAM Module for Meta Kaggle Pipeline
# Tạo tất cả IAM roles và policies cần thiết

# Data sources
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# Local values
locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.name
}

# ================================
# GLUE SERVICE ROLE
# ================================

# Trust policy cho Glue service
data "aws_iam_policy_document" "glue_trust_policy" {
  statement {
    effect = "Allow"
    
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
    
    actions = ["sts:AssumeRole"]
  }
}

# Glue execution role
resource "aws_iam_role" "glue_execution_role" {
  name               = "meta-glue-execution-role-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.glue_trust_policy.json
  
  tags = merge(var.tags, {
    Name    = "Meta Glue Execution Role"
    Purpose = "glue-etl"
    Service = "glue"
  })
}

# Custom policy cho Glue truy cập S3
data "aws_iam_policy_document" "glue_s3_policy" {
  # S3 access to data lake bucket
  statement {
    effect = "Allow"
    
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
      "s3:ListMultipartUploadParts"
    ]
    
    resources = [var.s3_bucket_arn]
  }
  
  statement {
    effect = "Allow"
    
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject"
    ]
    
    resources = ["${var.s3_bucket_arn}/*"]
  }
  
  # CloudWatch logs
  statement {
    effect = "Allow"
    
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:DescribeLogStreams"
    ]
    
    resources = [
      "arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws-glue/*"
    ]
  }
  
  # Glue catalog permissions
  statement {
    effect = "Allow"
    
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases", 
      "glue:CreateDatabase",
      "glue:UpdateDatabase",
      "glue:DeleteDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:CreatePartition",
      "glue:UpdatePartition",
      "glue:DeletePartition",
      "glue:BatchCreatePartition",
      "glue:BatchDeletePartition",
      "glue:BatchUpdatePartition"
    ]
    
    resources = [
      "arn:aws:glue:${local.region}:${local.account_id}:catalog",
      "arn:aws:glue:${local.region}:${local.account_id}:database/meta_*",
      "arn:aws:glue:${local.region}:${local.account_id}:table/meta_*/*"
    ]
  }
}

resource "aws_iam_policy" "glue_s3_policy" {
  name   = "meta-glue-s3-access-policy-${var.environment}"
  policy = data.aws_iam_policy_document.glue_s3_policy.json
  
  tags = var.tags
}

# Attach AWS managed policy cho Glue service
resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Attach custom S3 policy
resource "aws_iam_role_policy_attachment" "glue_s3_access" {
  role       = aws_iam_role.glue_execution_role.name
  policy_arn = aws_iam_policy.glue_s3_policy.arn
}

# ================================
# REDSHIFT SERVICE ROLE
# ================================

# Trust policy cho Redshift
data "aws_iam_policy_document" "redshift_trust_policy" {
  statement {
    effect = "Allow"
    
    principals {
      type        = "Service"
      identifiers = ["redshift.amazonaws.com"]
    }
    
    actions = ["sts:AssumeRole"]
  }
}

# Redshift service role
resource "aws_iam_role" "redshift_service_role" {
  name               = "meta-redshift-service-role-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.redshift_trust_policy.json
  
  tags = merge(var.tags, {
    Name    = "Meta Redshift Service Role"
    Purpose = "redshift-copy"
    Service = "redshift"
  })
}

# Custom policy cho Redshift COPY from S3
data "aws_iam_policy_document" "redshift_s3_policy" {
  statement {
    effect = "Allow"
    
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket"
    ]
    
    resources = [var.s3_bucket_arn]
  }
  
  statement {
    effect = "Allow"
    
    actions = [
      "s3:GetObject"
    ]
    
    resources = [
      "${var.s3_bucket_arn}/meta/gold/*",
      "${var.s3_bucket_arn}/scripts/sql/*"
    ]
  }
}

resource "aws_iam_policy" "redshift_s3_policy" {
  name   = "meta-redshift-s3-access-policy-${var.environment}"
  policy = data.aws_iam_policy_document.redshift_s3_policy.json
  
  tags = var.tags
}

# Attach custom S3 policy to Redshift role
resource "aws_iam_role_policy_attachment" "redshift_s3_access" {
  role       = aws_iam_role.redshift_service_role.name
  policy_arn = aws_iam_policy.redshift_s3_policy.arn
}

# ================================
# AIRFLOW USER (for orchestration)
# ================================

# IAM user cho Airflow
resource "aws_iam_user" "airflow_user" {
  name = "meta-airflow-user-${var.environment}"
  path = "/system/"
  
  tags = merge(var.tags, {
    Name    = "Meta Airflow User"
    Purpose = "airflow-orchestration"
    Service = "airflow"
  })
}

# Access key cho Airflow user
resource "aws_iam_access_key" "airflow_access_key" {
  user = aws_iam_user.airflow_user.name
}

# Policy cho Airflow user
data "aws_iam_policy_document" "airflow_policy" {
  # Glue permissions
  statement {
    effect = "Allow"
    
    actions = [
      "glue:StartJobRun",
      "glue:GetJobRun",
      "glue:BatchStopJobRun",
      "glue:GetJobRuns",
      "glue:StartCrawler",
      "glue:GetCrawler",
      "glue:GetCrawlerMetrics",
      "glue:StopCrawler",
      "glue:GetDatabases",
      "glue:GetTables"
    ]
    
    resources = [
      "arn:aws:glue:${local.region}:${local.account_id}:job/meta_*",
      "arn:aws:glue:${local.region}:${local.account_id}:crawler/meta_*",
      "arn:aws:glue:${local.region}:${local.account_id}:catalog",
      "arn:aws:glue:${local.region}:${local.account_id}:database/meta_*"
    ]
  }
  
  # S3 permissions
  statement {
    effect = "Allow"
    
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation"
    ]
    
    resources = [var.s3_bucket_arn]
  }
  
  statement {
    effect = "Allow"
    
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject"
    ]
    
    resources = [
      "${var.s3_bucket_arn}/meta/*",
      "${var.s3_bucket_arn}/scripts/*"
    ]
  }
  
  # Athena permissions
  statement {
    effect = "Allow"
    
    actions = [
      "athena:StartQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:StopQueryExecution",
      "athena:GetWorkGroup"
    ]
    
    resources = [
      "arn:aws:athena:${local.region}:${local.account_id}:workgroup/meta-*"
    ]
  }
  
  # CloudWatch permissions for monitoring
  statement {
    effect = "Allow"
    
    actions = [
      "cloudwatch:PutMetricData",
      "cloudwatch:GetMetricStatistics"
    ]
    
    resources = ["*"]
    
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["MetaPipeline"]
    }
  }
}

resource "aws_iam_policy" "airflow_policy" {
  name   = "meta-airflow-execution-policy-${var.environment}"
  policy = data.aws_iam_policy_document.airflow_policy.json
  
  tags = var.tags
}

# Attach policy to Airflow user
resource "aws_iam_user_policy_attachment" "airflow_policy_attachment" {
  user       = aws_iam_user.airflow_user.name
  policy_arn = aws_iam_policy.airflow_policy.arn
}

# ================================
# ================================
# META KAGGLE PIPELINE USER POLICY  
# ================================

# Data source để lấy current user info
data "aws_caller_identity" "current_user" {}

# Extract username from ARN
locals {
  # Extract username from arn:aws:iam::account:user/username
  current_user_arn = data.aws_caller_identity.current_user.arn
  current_username = regex("user/(.+)$", local.current_user_arn)[0]
}

# IAM Policy Document cho Meta Kaggle Pipeline permissions
data "aws_iam_policy_document" "meta_kaggle_pipeline_policy" {
  # Glue permissions
  statement {
    sid    = "GlueFullAccess"
    effect = "Allow"
    
    actions = [
      "glue:*",
      "glue:CreateDatabase",
      "glue:CreateJob", 
      "glue:CreateCrawler",
      "glue:CreatePartition",
      "glue:CreateTable",
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables", 
      "glue:GetJob",
      "glue:GetJobs",
      "glue:StartJobRun",
      "glue:GetJobRun",
      "glue:GetJobRuns",
      "glue:StartCrawler",
      "glue:GetCrawler",
      "glue:GetCrawlers"
    ]
    
    resources = ["*"]
  }
  
  # Athena permissions
  statement {
    sid    = "AthenaFullAccess"
    effect = "Allow"
    
    actions = [
      "athena:*",
      "athena:CreateWorkGroup",
      "athena:GetWorkGroup",
      "athena:UpdateWorkGroup", 
      "athena:DeleteWorkGroup",
      "athena:StartQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults"
    ]
    
    resources = ["*"]
  }
  
  # Redshift Serverless permissions
  statement {
    sid    = "RedshiftServerlessFullAccess"
    effect = "Allow"
    
    actions = [
      "redshift-serverless:*",
      "redshift-serverless:CreateNamespace",
      "redshift-serverless:CreateWorkgroup",
      "redshift-serverless:GetNamespace",
      "redshift-serverless:GetWorkgroup", 
      "redshift-serverless:UpdateWorkgroup",
      "redshift-serverless:CreateUsageLimit",
      "redshift-serverless:GetUsageLimit"
    ]
    
    resources = ["*"]
  }
  
  # Secrets Manager permissions (conditional)
  dynamic "statement" {
    for_each = var.enable_secrets_manager ? [1] : []
    
    content {
      sid    = "SecretsManagerAccess"
      effect = "Allow"
      
      actions = [
        "secretsmanager:CreateSecret",
        "secretsmanager:GetSecretValue",
        "secretsmanager:PutSecretValue", 
        "secretsmanager:UpdateSecret",
        "secretsmanager:DescribeSecret",
        "secretsmanager:DeleteSecret"
      ]
      
      resources = ["*"]
    }
  }
  
  # S3 và IAM permissions
  statement {
    sid    = "S3AndIAMAccess"
    effect = "Allow"
    
    actions = [
      "s3:*",
      "iam:GetRole",
      "iam:PassRole",
      "iam:ListRoles",
      "iam:GetRolePolicy",
      "iam:AttachRolePolicy",
      "iam:DetachRolePolicy"
    ]
    
    resources = ["*"]
  }
  
  # CloudWatch Logs permissions
  statement {
    sid    = "CloudWatchLogs"
    effect = "Allow"
    
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:DescribeLogGroups",
      "logs:DescribeLogStreams"
    ]
    
    resources = ["*"]
  }
}

# Tạo IAM Policy
resource "aws_iam_policy" "meta_kaggle_pipeline_policy" {
  name        = "MetaKagglePipelinePolicy-${var.environment}"
  path        = "/"
  description = "Custom policy for Meta Kaggle ETL Pipeline - Glue, Athena, Redshift Serverless access"
  
  policy = data.aws_iam_policy_document.meta_kaggle_pipeline_policy.json
  
  tags = merge(var.tags, {
    Name        = "Meta Kaggle Pipeline Policy"
    Purpose     = "pipeline-permissions"
    Service     = "iam"
    Environment = var.environment
  })
}

# Attach policy vào current user
resource "aws_iam_user_policy_attachment" "meta_kaggle_pipeline_attachment" {
  user       = local.current_username
  policy_arn = aws_iam_policy.meta_kaggle_pipeline_policy.arn
}

# ================================
# SECRETS MANAGER (for credentials)
# ================================

# Store Airflow credentials trong Secrets Manager (chỉ khi enable)
resource "aws_secretsmanager_secret" "airflow_credentials" {
  count = var.enable_secrets_manager ? 1 : 0
  
  name                    = "meta/airflow/aws-credentials/${var.environment}"
  description             = "AWS credentials cho Meta Airflow user"
  recovery_window_in_days = 7
  
  tags = merge(var.tags, {
    Name    = "Meta Airflow AWS Credentials"
    Purpose = "airflow-auth"
  })
}

resource "aws_secretsmanager_secret_version" "airflow_credentials" {
  count = var.enable_secrets_manager ? 1 : 0
  
  secret_id = aws_secretsmanager_secret.airflow_credentials[0].id
  
  secret_string = jsonencode({
    access_key_id     = aws_iam_access_key.airflow_access_key.id
    secret_access_key = aws_iam_access_key.airflow_access_key.secret
    region           = local.region
  })
}
