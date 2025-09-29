#!/bin/bash

# Script khởi tạo backend cho Terraform
# Chạy script này trước khi deploy infrastructure

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Configuration
REGION="us-west-1"
STATE_BUCKET="psi-de-terraform-state"
DYNAMODB_TABLE="terraform-state-locks"

echo -e "${BLUE}🔧 Terraform Backend Initialization${NC}"
echo -e "${BLUE}===================================${NC}"

print_status() {
    echo -e "${GREEN}✓${NC} $1"
}

print_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

# Check AWS CLI
if ! command -v aws &> /dev/null; then
    print_error "AWS CLI is not installed"
    exit 1
fi

# Check AWS credentials
if ! aws sts get-caller-identity &> /dev/null; then
    print_error "AWS credentials not configured"
    exit 1
fi

print_info "Creating S3 bucket for Terraform state..."

# Create S3 bucket
if ! aws s3api head-bucket --bucket "$STATE_BUCKET" 2>/dev/null; then
    aws s3 mb "s3://$STATE_BUCKET" --region "$REGION"
    print_status "S3 bucket created"
    
    # Enable versioning
    aws s3api put-bucket-versioning \
        --bucket "$STATE_BUCKET" \
        --versioning-configuration Status=Enabled
    print_status "Versioning enabled"
    
    # Block public access
    aws s3api put-public-access-block \
        --bucket "$STATE_BUCKET" \
        --public-access-block-configuration \
        "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
    print_status "Public access blocked"
    
else
    print_status "S3 bucket already exists"
fi

print_info "Creating DynamoDB table for state locking..."

# Create DynamoDB table
if ! aws dynamodb describe-table --table-name "$DYNAMODB_TABLE" --region "$REGION" &> /dev/null; then
    aws dynamodb create-table \
        --table-name "$DYNAMODB_TABLE" \
        --attribute-definitions AttributeName=LockID,AttributeType=S \
        --key-schema AttributeName=LockID,KeyType=HASH \
        --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 \
        --region "$REGION" > /dev/null
    
    print_info "Waiting for DynamoDB table to be active..."
    aws dynamodb wait table-exists --table-name "$DYNAMODB_TABLE" --region "$REGION"
    print_status "DynamoDB table created"
else
    print_status "DynamoDB table already exists"
fi

echo -e "\n${GREEN}🎉 Backend resources created successfully!${NC}"
echo -e "\n${YELLOW}Next steps:${NC}"
echo "1. Uncomment the backend configuration trong main.tf"
echo "2. Run terraform init để migrate state to S3"
echo "3. Run terraform plan và apply để deploy infrastructure"

echo -e "\n${BLUE}Backend Resources:${NC}"
echo "• S3 Bucket: s3://$STATE_BUCKET"
echo "• DynamoDB Table: $DYNAMODB_TABLE"
echo "• Region: $REGION"