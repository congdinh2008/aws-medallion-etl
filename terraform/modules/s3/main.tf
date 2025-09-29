# S3 Module for Meta Kaggle Data Lake
# Tạo S3 bucket với cấu trúc Medallion Architecture

# Main S3 bucket cho data lake
resource "aws_s3_bucket" "meta_data_lake" {
  bucket = var.bucket_name
  
  tags = merge(var.tags, {
    Name        = "Meta Kaggle Data Lake"
    Layer       = "Storage"
    Purpose     = "data-lake"
    DataClass   = "processed"
  })
}

# Versioning configuration
resource "aws_s3_bucket_versioning" "meta_versioning" {
  bucket = aws_s3_bucket.meta_data_lake.id
  
  versioning_configuration {
    status = "Enabled"
  }
}

# Server-side encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "meta_encryption" {
  bucket = aws_s3_bucket.meta_data_lake.id
  
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

# Block all public access
resource "aws_s3_bucket_public_access_block" "meta_pab" {
  bucket = aws_s3_bucket.meta_data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Intelligent tiering for cost optimization
resource "aws_s3_bucket_intelligent_tiering_configuration" "meta_tiering" {
  bucket = aws_s3_bucket.meta_data_lake.id
  name   = "meta-intelligent-tiering"
  
  status = var.enable_cost_optimization ? "Enabled" : "Disabled"
  
  filter {
    prefix = "meta/"
  }
  
  tiering {
    access_tier = "ARCHIVE_ACCESS"
    days        = 90
  }
  
  tiering {
    access_tier = "DEEP_ARCHIVE_ACCESS"
    days        = 180
  }
}

# Lifecycle configuration
resource "aws_s3_bucket_lifecycle_configuration" "meta_lifecycle" {
  bucket = aws_s3_bucket.meta_data_lake.id
  
  # Raw data lifecycle
  rule {
    id     = "raw_data_lifecycle"
    status = "Enabled"
    
    filter {
      prefix = "meta/raw/"
    }
    
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
    
    transition {
      days          = 365
      storage_class = "DEEP_ARCHIVE"
    }
  }
  
  # Bronze layer lifecycle
  rule {
    id     = "bronze_data_lifecycle"
    status = "Enabled"
    
    filter {
      prefix = "meta/bronze/"
    }
    
    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }
    
    transition {
      days          = 180
      storage_class = "GLACIER"
    }
  }
  
  # Silver layer lifecycle
  rule {
    id     = "silver_data_lifecycle" 
    status = "Enabled"
    
    filter {
      prefix = "meta/silver/"
    }
    
    transition {
      days          = 180
      storage_class = "STANDARD_IA"
    }
  }
  
  # Cleanup incomplete multipart uploads
  rule {
    id     = "incomplete_multipart_uploads"
    status = "Enabled"
    
    filter {}
    
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
  
  # Cleanup old rejected records
  rule {
    id     = "rejected_records_cleanup"
    status = "Enabled"
    
    filter {
      prefix = "meta/bronze/_rejects/"
    }
    
    expiration {
      days = 30
    }
  }
}

# Local values cho folder structure
locals {
  tables = ["users", "datasets", "competitions", "tags", "kernels"]
  layers = ["raw", "bronze", "silver", "gold"]
  
  # Tạo combinations của layers và tables
  folder_combinations = flatten([
    for layer in local.layers : [
      for table in local.tables : {
        key   = "${layer}_${table}"
        layer = layer
        table = table
      }
    ]
  ])
  
  # Special folders
  special_folders = [
    "meta/bronze/_rejects/",
    "meta/bronze/_reports/",
    "meta/silver/_reports/", 
    "meta/gold/_reports/",
    "meta/gold/_logs/",
    "meta/athena-results/",
    "meta/_monitoring/",
    "scripts/glue_jobs/",
    "scripts/sql/",
    "scripts/setup/"
  ]
}

# Tạo folder structure cho từng table và layer
resource "aws_s3_object" "layer_table_folders" {
  for_each = {
    for item in local.folder_combinations : item.key => item
  }
  
  bucket       = aws_s3_bucket.meta_data_lake.id
  key          = "meta/${each.value.layer}/${each.value.table}/"
  content_type = "application/x-directory"
  
  tags = merge(var.tags, {
    Layer = each.value.layer
    Table = each.value.table
  })
}

# Tạo special folders
resource "aws_s3_object" "special_folders" {
  for_each = toset(local.special_folders)
  
  bucket       = aws_s3_bucket.meta_data_lake.id
  key          = each.value
  content_type = "application/x-directory"
  
  tags = var.tags
}

# Notification configuration cho monitoring
resource "aws_s3_bucket_notification" "meta_bucket_notifications" {
  bucket = aws_s3_bucket.meta_data_lake.id
  
  # Lambda function notification for file uploads (if needed)
  # Có thể extend sau để trigger processing
}

# CORS configuration cho web access (nếu cần)
resource "aws_s3_bucket_cors_configuration" "meta_cors" {
  bucket = aws_s3_bucket.meta_data_lake.id
  
  cors_rule {
    allowed_headers = ["*"]
    allowed_methods = ["GET", "HEAD"]
    allowed_origins = ["*"]
    expose_headers  = ["ETag"]
    max_age_seconds = 3000
  }
}
