# 🚀 Meta Kaggle Pipeline - Terraform Infrastructure

## 📋 **OVERVIEW**

Complete **Infrastructure as Code** implementation cho Meta Kaggle ETL Pipeline sử dụng **Medallion Architecture** (Raw → Bronze → Silver → Gold) với:

- **AWS S3**: Data Lake storage với intelligent tiering
- **AWS Glue**: ETL jobs và Data Catalog
- **AWS Athena**: Interactive analytics workgroup
- **AWS Redshift Serverless**: Data warehouse với cost controls
- **IAM**: Comprehensive security với **auto-policy assignment**

## 🏗️ **ARCHITECTURE**

```
┌─── Raw Data (CSV) ───┐    ┌─── Bronze Layer ───┐    ┌─── Silver Layer ───┐    ┌─── Gold Layer ───┐
│   • Meta datasets    │ => │  • Schema validation│ => │  • Data cleaning   │ => │  • Business KPIs  │
│   • Competitions     │    │  • Metadata tags    │    │  • Deduplication   │    │  • Dimensional    │
│   • Users/Tags       │    │  • Partitioning     │    │  • Standardization │    │  • Fact tables    │
└─────────────────────┘    └───────────────────┘    └──────────────────┘    └─────────────────┘
```

## ⚡ **QUICK START**

### **1. Prerequisites**
```bash
# AWS CLI configured
aws configure list

# Terraform installed  
terraform version  # >= 1.0

# Proper permissions (auto-granted by deployment)
aws sts get-caller-identity
```

### **2. Deploy Infrastructure** 
```bash
cd terraform/environments/dev

# Review configuration
cat terraform.tfvars

# Initialize Terraform
terraform init

# Plan deployment (foundation + auto-permissions)
terraform plan -var-file='terraform.tfvars'

# Deploy complete infrastructure 
terraform apply -var-file='terraform.tfvars'
```

### **3. Verify Resources**
```bash
# Check S3 Data Lake
aws s3 ls s3://psi-de-glue-congdinh/

# Verify IAM permissions  
terraform output current_user_with_permissions

# List Glue resources (if deployed)
terraform output glue_databases
terraform output glue_crawlers
```

## 🎯 **KEY FEATURES**

### **🔐 Automatic Permissions Management**
- **Self-granting IAM policy** tự động được tạo và attach vào current user
- Không cần admin intervention hoặc manual script execution
- Full permissions cho Glue, Athena, Redshift Serverless

### **🎛️ Conditional Deployment**
```hcl
# Deploy only foundation (S3 + IAM)
auto_deploy_advanced_services = false

# Deploy complete pipeline (S3 + IAM + Glue + Redshift)  
auto_deploy_advanced_services = true
```

### **💰 Built-in Cost Controls**
- **Redshift Serverless**: 500 RPU-hours/month limit ($125-225 cap)
- **S3**: Intelligent tiering, lifecycle policies
- **Glue**: Optimized worker configurations
- **Monthly budget estimate**: $157-295

### **🔄 Production-Ready Patterns**
- **Idempotent operations**: Safe re-runs với Terraform state
- **Environment separation**: dev/staging/prod configurations
- **Comprehensive tagging**: Cost allocation, resource tracking
- **Security best practices**: Least privilege IAM policies

## 📁 **DIRECTORY STRUCTURE**

```
terraform/
├── environments/dev/           # Environment-specific config
│   ├── main.tf                # Main deployment configuration
│   ├── variables.tf           # Input variables with validation
│   ├── outputs.tf             # Resource outputs
│   └── terraform.tfvars       # Environment values
├── modules/                   # Reusable modules
│   ├── s3/                   # Data Lake với Medallion structure  
│   ├── iam/                  # Security + Auto-permissions
│   ├── glue/                 # ETL jobs + Data Catalog
│   └── redshift/             # Serverless data warehouse
├── init-backend.sh           # Remote state setup
├── deploy.sh                 # Integrated deployment script
└── README.md                 # This documentation
```

## 🔧 **CONFIGURATION**

### **Core Variables** (`terraform.tfvars`)
```hcl
# Environment settings
environment = "dev"
aws_region  = "us-west-1"

# S3 Data Lake  
s3_bucket_name = "psi-de-glue-congdinh"

# Deployment control
auto_deploy_advanced_services = true    # Set false for foundation-only

# Redshift Serverless settings
redshift_base_capacity_rpus = 8
redshift_monthly_usage_limit = 500      # RPU-hours cap
```

### **Advanced Options**
```hcl
# Optional: Disable Secrets Manager
enable_secrets_manager = false

# Optional: Network configuration cho Redshift
publicly_accessible = false
private_subnet_ids = ["subnet-xxx", "subnet-yyy"] 
```

## 📊 **RESOURCE OUTPUTS**

```bash
# Foundation resources
terraform output s3_bucket_name
terraform output airflow_user_access_key_id

# Pipeline permissions  
terraform output meta_kaggle_pipeline_policy_arn
terraform output current_user_with_permissions

# Advanced services (if deployed)
terraform output glue_databases
terraform output redshift_namespace_name
terraform output redshift_estimated_monthly_cost
```

## 🚨 **DEPLOYMENT SCENARIOS**

### **Scenario 1: Foundation First** 
```bash
# Set in terraform.tfvars
auto_deploy_advanced_services = false

terraform apply -var-file='terraform.tfvars'
# Deploys: S3 + IAM + Auto-permissions

# Later, deploy advanced services
auto_deploy_advanced_services = true
terraform apply -var-file='terraform.tfvars'  
```

### **Scenario 2: Complete Pipeline**
```bash
# Default: Full deployment 
terraform apply -var-file='terraform.tfvars'
# Deploys: S3 + IAM + Auto-permissions + Glue + Redshift
```

### **Scenario 3: Permission Issues**
```bash
# If admin permissions needed
terraform apply -target=module.iam
# Then retry full deployment
terraform apply -var-file='terraform.tfvars'
```

## 💡 **BEST PRACTICES**

### **🏷️ Resource Tagging**
```hcl
common_tags = {
  Project     = "meta-kaggle-pipeline"
  Environment = "dev"
  ManagedBy   = "terraform"
  CostCenter  = "data-engineering"
  Owner       = "data-team"
}
```

### **🔒 Security**
- IAM policies follow **least privilege** principle
- Sensitive outputs marked as `sensitive = true`
- Resource-specific permissions với arn-based constraints

### **💰 Cost Management**
- Usage limits implemented for all chargeable services
- Automatic resource cleanup policies  
- Monthly cost estimation in outputs
- Intelligent storage tiering

## 🛠️ **TROUBLESHOOTING**

### **Permission Denied Errors**
```bash
# The IAM policy should auto-grant permissions
# If still failing, check:
terraform output current_user_with_permissions

# Manual policy verification
aws iam list-attached-user-policies --user-name $(terraform output -raw current_user_with_permissions)
```

### **Glue Job Failures** 
```bash
# Check if advanced services deployed
terraform output glue_databases

# Verify S3 bucket access
aws s3 ls s3://$(terraform output -raw s3_bucket_name)/
```

### **State Management**
```bash
# Remote backend setup (optional)
./init-backend.sh

# State recovery
terraform import <resource> <id>
```

## 🔄 **PIPELINE INTEGRATION**

### **Airflow Configuration**
```python
# Use Terraform outputs in Airflow DAGs
from airflow.models import Variable

BUCKET_NAME = Variable.get("S3_BUCKET_NAME", default_var="psi-de-glue-congdinh")
AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")  # From terraform output
GLUE_ROLE_ARN = Variable.get("GLUE_ROLE_ARN")
```

### **Data Pipeline Flow**
1. **Raw ingestion** → S3 raw/ folder
2. **Bronze jobs** → Schema validation, metadata tagging  
3. **Silver jobs** → Data cleaning, deduplication
4. **Gold jobs** → Business metrics, dimensional modeling
5. **Analytics** → Athena workgroup queries

## 🎯 **PRODUCTION CHECKLIST**

- [ ] **Remote state**: Configure S3 backend với DynamoDB locking
- [ ] **Multiple environments**: Separate dev/staging/prod workspaces  
- [ ] **CI/CD integration**: Automate terraform plan/apply
- [ ] **Monitoring**: CloudWatch alarms cho resource usage
- [ ] **Backup strategy**: Cross-region replication cho critical data
- [ ] **Cost alerts**: Budget notifications setup
- [ ] **Security review**: IAM policies audit, VPC configuration

## 📞 **SUPPORT**

### **Common Issues**
- **Resource limits**: Check AWS service quotas
- **Networking**: Ensure proper VPC/subnet configuration
- **Costs**: Monitor usage với AWS Cost Explorer

### **Documentation**
- **Terraform modules**: See individual module READMEs
- **AWS services**: Official AWS documentation  
- **Pipeline logic**: Check `dags/` directory trong Airflow project

---

**💡 This infrastructure follows enterprise patterns với production-ready configurations. Modify variables trong `terraform.tfvars` để customize cho specific requirements.**