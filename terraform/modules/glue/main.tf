# Glue Module for Meta Kaggle Pipeline
# Tạo Glue Data Catalog, Databases, Crawlers và Job configurations

# Local values
locals {
  tables = ["users", "datasets", "competitions", "tags", "kernels"]
  
  # Dimension tables cho Gold layer
  dimensions = [
    "dim_user", "dim_dataset", "dim_competition", 
    "dim_tag", "dim_date"
  ]
  
  # Fact tables cho Gold layer
  facts = [
    "fact_dataset_owner_daily",
    "fact_competitions_yearly", 
    "fact_tag_usage_daily",
    "bridge_dataset_tag"
  ]
}

# ================================
# GLUE DATABASES
# ================================

# Bronze database
resource "aws_glue_catalog_database" "bronze_database" {
  name        = "meta_bronze"
  description = "Bronze layer database cho Meta Kaggle raw data với metadata"
  
  create_table_default_permission {
    permissions = ["ALL"]
    
    principal {
      data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS"
    }
  }
  
  tags = merge(var.tags, {
    Name  = "Meta Bronze Database"
    Layer = "bronze"
  })
}

# Silver database
resource "aws_glue_catalog_database" "silver_database" {
  name        = "meta_silver" 
  description = "Silver layer database cho Meta Kaggle cleaned và enriched data"
  
  create_table_default_permission {
    permissions = ["ALL"]
    
    principal {
      data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS"
    }
  }
  
  tags = merge(var.tags, {
    Name  = "Meta Silver Database"
    Layer = "silver"
  })
}

# Gold database
resource "aws_glue_catalog_database" "gold_database" {
  name        = "meta_gold"
  description = "Gold layer database cho Meta Kaggle dimensional model và business metrics"
  
  create_table_default_permission {
    permissions = ["ALL"]
    
    principal {
      data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS"
    }
  }
  
  tags = merge(var.tags, {
    Name  = "Meta Gold Database"  
    Layer = "gold"
  })
}

# ================================
# BRONZE LAYER CRAWLERS
# ================================

# Bronze crawlers - một crawler cho mỗi table
resource "aws_glue_crawler" "bronze_table_crawlers" {
  for_each = toset(local.tables)
  
  name          = "meta_bronze_${each.key}_crawler"
  role          = var.glue_role_arn
  database_name = aws_glue_catalog_database.bronze_database.name
  description   = "Crawler cho Bronze ${each.key} table"
  
  s3_target {
    path = "s3://${var.s3_bucket_name}/meta/bronze/${each.key}/"
  }
  
  # Schema change policy
  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
  
  # Configuration cho partition discovery
  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = {
        AddOrUpdateBehavior = "InheritFromTable"
      }
      Tables = {
        AddOrUpdateBehavior = "MergeNewColumns"
      }
    }
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"  
    }
  })
  
  tags = merge(var.tags, {
    Name  = "Bronze ${each.key} Crawler"
    Layer = "bronze"
    Table = each.key
  })
}

# ================================
# SILVER LAYER CRAWLERS
# ================================

# Silver crawlers
resource "aws_glue_crawler" "silver_table_crawlers" {
  for_each = toset(local.tables)
  
  name          = "meta_silver_${each.key}_crawler"
  role          = var.glue_role_arn
  database_name = aws_glue_catalog_database.silver_database.name
  description   = "Crawler cho Silver ${each.key} table"
  
  s3_target {
    path = "s3://${var.s3_bucket_name}/meta/silver/${each.key}/"
  }
  
  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
  
  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = {
        AddOrUpdateBehavior = "InheritFromTable"
      }
    }
  })
  
  tags = merge(var.tags, {
    Name  = "Silver ${each.key} Crawler"
    Layer = "silver"
    Table = each.key
  })
}

# ================================
# GOLD LAYER CRAWLERS
# ================================

# Gold dimension crawlers
resource "aws_glue_crawler" "gold_dimension_crawlers" {
  for_each = toset(local.dimensions)
  
  name          = "meta_gold_${each.key}_crawler"
  role          = var.glue_role_arn
  database_name = aws_glue_catalog_database.gold_database.name
  description   = "Crawler cho Gold ${each.key} dimension"
  
  s3_target {
    path = "s3://${var.s3_bucket_name}/meta/gold/${each.key}/"
  }
  
  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "DEPRECATE_IN_DATABASE"  # Preserve dimension history
  }
  
  tags = merge(var.tags, {
    Name  = "Gold ${each.key} Crawler"
    Layer = "gold"
    Type  = "dimension"
    Table = each.key
  })
}

# Gold fact crawlers
resource "aws_glue_crawler" "gold_fact_crawlers" {
  for_each = toset(local.facts)
  
  name          = "meta_gold_${each.key}_crawler"
  role          = var.glue_role_arn
  database_name = aws_glue_catalog_database.gold_database.name
  description   = "Crawler cho Gold ${each.key} fact table"
  
  s3_target {
    path = "s3://${var.s3_bucket_name}/meta/gold/${each.key}/"
  }
  
  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
  
  # Partition detection cho fact tables
  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = {
        AddOrUpdateBehavior = "InheritFromTable"
      }
    }
  })
  
  tags = merge(var.tags, {
    Name  = "Gold ${each.key} Crawler"
    Layer = "gold"
    Type  = "fact"
    Table = each.key
  })
}

# ================================
# GLUE JOBS (Placeholder - actual scripts uploaded separately)
# ================================

# Bronze transformation jobs
resource "aws_glue_job" "bronze_transformation_jobs" {
  for_each = toset(local.tables)
  
  name         = "meta_bronze_${each.key}"
  role_arn     = var.glue_role_arn
  description  = "Bronze transformation job cho ${each.key} table"
  glue_version = "4.0"  # Latest Glue version
  
  command {
    script_location = "s3://${var.s3_bucket_name}/scripts/glue_jobs/bronze_transform_${each.key}.py"
    python_version  = "3"
  }
  
  # Resource configuration
  max_retries       = 2
  timeout           = 60  # 60 minutes
  number_of_workers = 2
  worker_type       = "G.1X"  # 4 vCPU, 16 GB memory
  
  # Job parameters
  default_arguments = {
    "--TempDir"                          = "s3://${var.s3_bucket_name}/tmp/glue-jobs/"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--job-language"                     = "python"
  }
  
  tags = merge(var.tags, {
    Name  = "Bronze ${each.key} Job"
    Layer = "bronze"
    Table = each.key
  })
}

# Silver transformation jobs
resource "aws_glue_job" "silver_transformation_jobs" {
  for_each = toset(local.tables)
  
  name         = "meta_silver_${each.key}"
  role_arn     = var.glue_role_arn
  description  = "Silver transformation job cho ${each.key} table"
  glue_version = "4.0"
  
  command {
    script_location = "s3://${var.s3_bucket_name}/scripts/glue_jobs/silver_transform_${each.key}.py"
    python_version  = "3"
  }
  
  max_retries       = 2
  timeout           = 90  # Silver jobs may take longer
  number_of_workers = 3
  worker_type       = "G.1X"
  
  default_arguments = {
    "--TempDir"                          = "s3://${var.s3_bucket_name}/tmp/glue-jobs/"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--job-language"                     = "python"
  }
  
  tags = merge(var.tags, {
    Name  = "Silver ${each.key} Job"
    Layer = "silver"
    Table = each.key
  })
}

# Gold dimension jobs
resource "aws_glue_job" "gold_dimension_jobs" {
  for_each = toset(local.dimensions)
  
  name         = "meta_gold_${each.key}"
  role_arn     = var.glue_role_arn
  description  = "Gold dimension job cho ${each.key}"
  glue_version = "4.0"
  
  command {
    script_location = "s3://${var.s3_bucket_name}/scripts/glue_jobs/gold_${each.key}.py"
    python_version  = "3"
  }
  
  max_retries       = 2
  timeout           = 60
  number_of_workers = 2
  worker_type       = "G.1X"
  
  default_arguments = {
    "--TempDir"                          = "s3://${var.s3_bucket_name}/tmp/glue-jobs/"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"  # Full refresh for dimensions
    "--job-language"                     = "python"
  }
  
  tags = merge(var.tags, {
    Name  = "Gold ${each.key} Job"
    Layer = "gold"
    Type  = "dimension"
    Table = each.key
  })
}

# Gold fact jobs
resource "aws_glue_job" "gold_fact_jobs" {
  for_each = toset(local.facts)
  
  name         = "meta_gold_${each.key}"
  role_arn     = var.glue_role_arn
  description  = "Gold fact job cho ${each.key}"
  glue_version = "4.0"
  
  command {
    script_location = "s3://${var.s3_bucket_name}/scripts/glue_jobs/gold_${each.key}.py"
    python_version  = "3"
  }
  
  max_retries       = 2
  timeout           = 90
  number_of_workers = 3
  worker_type       = "G.1X"
  
  default_arguments = {
    "--TempDir"                          = "s3://${var.s3_bucket_name}/tmp/glue-jobs/"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--job-language"                     = "python"
  }
  
  tags = merge(var.tags, {
    Name  = "Gold ${each.key} Job"
    Layer = "gold"
    Type  = "fact"
    Table = each.key
  })
}

# ================================
# ATHENA WORKGROUP
# ================================

# Workgroup cho Meta analytics
resource "aws_athena_workgroup" "meta_analytics" {
  name        = "meta-analytics"
  description = "Workgroup cho Meta Kaggle analytics queries"
  
  configuration {
    enforce_workgroup_configuration = true
    
    result_configuration {
      output_location = "s3://${var.s3_bucket_name}/meta/athena-results/"
      
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
  
  tags = merge(var.tags, {
    Name    = "Meta Analytics Workgroup"
    Purpose = "analytics"
  })
}
