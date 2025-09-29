#!/bin/bash

#!/bin/bash

# Meta Kaggle Pipeline - Integrated Terraform Deployment Script
# Supports: Foundation-only deployment, Full pipeline deployment, Error handling, Validation

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# Configuration
TERRAFORM_DIR="$(pwd)/environments/dev"
TFVARS_FILE="terraform.tfvars"
STATE_BUCKET="psi-de-terraform-state"

# Functions
print_header() {
    echo -e "${PURPLE}"
    echo "╔══════════════════════════════════════════════════════════════════════════════════╗"
    echo "║                    🚀 Meta Kaggle Pipeline Deployment                           ║"
    echo "║                        Terraform Infrastructure as Code                          ║"
    echo "╚══════════════════════════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

print_status() {
    echo -e "${GREEN}✓${NC} $1"
}

print_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

show_usage() {
    echo -e "${BLUE}Usage:${NC} $0 [OPTIONS]"
    echo ""
    echo "OPTIONS:"
    echo "  --foundation-only     Deploy only S3 + IAM foundation"
    echo "  --full-pipeline      Deploy complete infrastructure (default)"
    echo "  --validate           Validate configuration without deployment"
    echo "  --destroy           Destroy all infrastructure"
    echo "  --plan              Show deployment plan"
    echo "  --init-backend      Initialize remote state backend"
    echo "  --help              Show this help message"
    echo ""
    echo "EXAMPLES:"
    echo "  $0                           # Full deployment"
    echo "  $0 --foundation-only         # Deploy foundation first"
    echo "  $0 --validate               # Validate without changes"
    echo "  $0 --plan                   # Show plan without applying"
}

# Function to generate secure Redshift password
generate_redshift_password() {
    # Generate a secure password that meets AWS Redshift requirements:
    # - 8-64 characters
    # - At least 1 uppercase, 1 lowercase, 1 number
    # - No forbidden characters: / @ " space \ '
    
    local upper="ABCDEFGHJKLMNPQRSTUVWXYZ"  # Avoid confusing chars
    local lower="abcdefghijkmnpqrstuvwxyz"  # Avoid confusing chars
    local numbers="23456789"                # Avoid 0, 1
    local special="!#%&*+,./:;=?^_{}~-"     # AWS allowed special chars
    
    # Ensure at least one of each required type
    local password=""
    password+="$(echo "$upper" | fold -w1 | shuf -n1)"      # 1 uppercase
    password+="$(echo "$lower" | fold -w1 | shuf -n1)"      # 1 lowercase  
    password+="$(echo "$numbers" | fold -w1 | shuf -n1)"    # 1 number
    password+="$(echo "$special" | fold -w1 | shuf -n1)"    # 1 special
    
    # Fill remaining characters (total 12 characters)
    local all_chars="${upper}${lower}${numbers}${special}"
    for i in {1..8}; do
        password+="$(echo "$all_chars" | fold -w1 | shuf -n1)"
    done
    
    # Shuffle the password
    echo "$password" | fold -w1 | shuf | tr -d '\n'
}

check_prerequisites() {
    print_info "Checking prerequisites..."
    
    # Check AWS CLI
    if ! command -v aws &> /dev/null; then
        print_error "AWS CLI is not installed"
        exit 1
    fi
    
    # Check Terraform
    if ! command -v terraform &> /dev/null; then
        print_error "Terraform is not installed"
        exit 1
    fi
    
    # Check AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        print_error "AWS credentials not configured or invalid"
        exit 1
    fi
    
    # Check Terraform directory
    if [ ! -d "$TERRAFORM_DIR" ]; then
        print_error "Terraform directory not found: $TERRAFORM_DIR"
        exit 1
    fi
    
    # Check tfvars file
    if [ ! -f "$TERRAFORM_DIR/$TFVARS_FILE" ]; then
        print_error "Terraform variables file not found: $TERRAFORM_DIR/$TFVARS_FILE"
        exit 1
    fi
    
    print_status "Prerequisites check passed"
    
    # Show current AWS identity
    CURRENT_USER=$(aws sts get-caller-identity --query "Arn" --output text)
    print_info "Current AWS identity: $CURRENT_USER"
}

init_backend() {
    print_info "Initializing remote state backend..."
    
    # Check if backend script exists
    if [ -f "./init-backend.sh" ]; then
        chmod +x ./init-backend.sh
        ./init-backend.sh
    else
        print_warning "Backend initialization script not found, proceeding with local state"
    fi
}

validate_terraform() {
    print_info "Validating Terraform configuration..."
    
    cd "$TERRAFORM_DIR"
    
    # Initialize if needed
    if [ ! -d ".terraform" ]; then
        print_info "Initializing Terraform..."
        terraform init
    fi
    
    # Validate configuration
    if terraform validate; then
        print_status "Terraform configuration is valid"
    else
        print_error "Terraform configuration validation failed"
        exit 1
    fi
    
    # Format check
    if terraform fmt -check -recursive .; then
        print_status "Terraform formatting is correct"
    else
        print_warning "Terraform formatting issues found. Running terraform fmt..."
        terraform fmt -recursive .
    fi
}

show_deployment_plan() {
    print_info "Generating deployment plan..."
    
    cd "$TERRAFORM_DIR"
    
    local FOUNDATION_ONLY=$1
    local PLAN_ARGS=""
    
    if [ "$FOUNDATION_ONLY" = "true" ]; then
        PLAN_ARGS="-var='auto_deploy_advanced_services=false'"
        print_info "Planning foundation-only deployment (S3 + IAM)"
    else
        print_info "Planning full pipeline deployment"
    fi
    
    echo -e "${BLUE}════════════ TERRAFORM PLAN ════════════${NC}"
    eval "terraform plan -var-file='$TFVARS_FILE' $PLAN_ARGS -detailed-exitcode" || {
        PLAN_EXIT_CODE=$?
        if [ $PLAN_EXIT_CODE -eq 2 ]; then
            print_info "Plan shows changes to be made"
            return 0
        else
            print_error "Terraform plan failed"
            return 1
        fi
    }
    echo -e "${BLUE}═══════════════════════════════════════${NC}"
}

deploy_infrastructure() {
    local FOUNDATION_ONLY=$1
    local APPLY_ARGS=""
    
    cd "$TERRAFORM_DIR"
    
    if [ "$FOUNDATION_ONLY" = "true" ]; then
        APPLY_ARGS="-var='auto_deploy_advanced_services=false'"
        print_info "Deploying foundation infrastructure (S3 + IAM + Auto-permissions)..."
    else
        print_info "Deploying complete pipeline infrastructure..."
    fi
    
    print_warning "This will create/modify AWS resources and may incur costs."
    echo -n "Continue with deployment? (y/N): "
    read -r response
    
    if [[ ! "$response" =~ ^[Yy]$ ]]; then
        print_info "Deployment cancelled by user"
        exit 0
    fi
    
    echo -e "${BLUE}════════════ TERRAFORM APPLY ════════════${NC}"
    if eval "terraform apply -var-file='$TFVARS_FILE' $APPLY_ARGS -auto-approve"; then
        print_status "Infrastructure deployment completed successfully!"
    else
        print_error "Infrastructure deployment failed"
        exit 1
    fi
    echo -e "${BLUE}════════════════════════════════════════${NC}"
}

show_outputs() {
    print_info "Retrieving deployment outputs..."
    
    cd "$TERRAFORM_DIR"
    
    echo -e "${BLUE}════════════ DEPLOYMENT OUTPUTS ═════════════${NC}"
    terraform output
    echo -e "${BLUE}═══════════════════════════════════════════════${NC}"
    
    # Extract key information
    print_info "Key deployment information:"
    
    # S3 bucket
    if terraform output -raw s3_bucket_name &>/dev/null; then
        BUCKET_NAME=$(terraform output -raw s3_bucket_name)
        print_status "S3 Data Lake: s3://$BUCKET_NAME"
    fi
    
    # Current user with permissions
    if terraform output -raw current_user_with_permissions &>/dev/null; then
        CURRENT_USER=$(terraform output -raw current_user_with_permissions)
        print_status "User with permissions: $CURRENT_USER"
    fi
    
    # Airflow credentials
    if terraform output -raw airflow_user_access_key_id &>/dev/null; then
        ACCESS_KEY=$(terraform output -raw airflow_user_access_key_id)
        print_status "Airflow Access Key: $ACCESS_KEY"
        print_warning "Secret key is sensitive, use: terraform output airflow_user_secret_access_key"
    fi
    
    # Cost estimation
    if terraform output -raw redshift_estimated_monthly_cost &>/dev/null; then
        COST_ESTIMATE=$(terraform output redshift_estimated_monthly_cost)
        print_info "Estimated monthly cost: $COST_ESTIMATE"
    fi
}

verify_deployment() {
    print_info "Verifying deployment..."
    
    cd "$TERRAFORM_DIR"
    
    # Check S3 bucket
    if terraform output -raw s3_bucket_name &>/dev/null; then
        BUCKET_NAME=$(terraform output -raw s3_bucket_name)
        if aws s3 ls "s3://$BUCKET_NAME" &>/dev/null; then
            print_status "S3 bucket is accessible"
            
            # Check folder structure
            if aws s3 ls "s3://$BUCKET_NAME/meta/" &>/dev/null; then
                print_status "S3 Medallion folder structure verified"
            fi
        else
            print_warning "S3 bucket access verification failed"
        fi
    fi
    
    # Check IAM policy
    if terraform output -raw meta_kaggle_pipeline_policy_arn &>/dev/null; then
        POLICY_ARN=$(terraform output -raw meta_kaggle_pipeline_policy_arn)
        if aws iam get-policy --policy-arn "$POLICY_ARN" &>/dev/null; then
            print_status "IAM pipeline policy exists and is accessible"
        else
            print_warning "IAM policy verification failed"
        fi
    fi
    
    # Check advanced services (if deployed)
    if terraform output -raw glue_databases 2>/dev/null | grep -q "meta_"; then
        print_status "Glue databases verified"
    else
        print_info "Glue databases not deployed (foundation-only mode or deployment pending)"
    fi
    
    if terraform output -raw redshift_namespace_name &>/dev/null; then
        NAMESPACE=$(terraform output -raw redshift_namespace_name)
        if [ "$NAMESPACE" != "null" ]; then
            print_status "Redshift Serverless namespace verified"
        fi
    else
        print_info "Redshift Serverless not deployed (foundation-only mode or deployment pending)"
    fi
}

destroy_infrastructure() {
    print_warning "⚠️  DESTRUCTIVE OPERATION ⚠️"
    print_warning "This will destroy ALL infrastructure resources including:"
    print_warning "  - S3 buckets and all data"
    print_warning "  - Glue databases and jobs"
    print_warning "  - Redshift Serverless clusters"
    print_warning "  - IAM roles and policies"
    print_warning ""
    print_error "THIS ACTION CANNOT BE UNDONE!"
    echo ""
    echo -n "Type 'DESTROY' to confirm destruction: "
    read -r confirmation
    
    if [ "$confirmation" != "DESTROY" ]; then
        print_info "Destruction cancelled"
        exit 0
    fi
    
    cd "$TERRAFORM_DIR"
    
    echo -e "${RED}════════════ TERRAFORM DESTROY ════════════${NC}"
    if terraform destroy -var-file="$TFVARS_FILE" -auto-approve; then
        print_status "Infrastructure destroyed successfully"
    else
        print_error "Infrastructure destruction failed"
        exit 1
    fi
    echo -e "${RED}═══════════════════════════════════════════${NC}"
}

# Main script
main() {
    print_header
    
    # Parse arguments
    FOUNDATION_ONLY=false
    VALIDATE_ONLY=false
    PLAN_ONLY=false
    DESTROY_MODE=false
    INIT_BACKEND_MODE=false
    DEPLOYMENT_MODE="foundation"  # default to foundation mode
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --foundation-only)
                FOUNDATION_ONLY=true
                DEPLOYMENT_MODE="foundation"
                shift
                ;;
            --full-pipeline)
                FOUNDATION_ONLY=false
                DEPLOYMENT_MODE="full"
                shift
                ;;
            --validate)
                VALIDATE_ONLY=true
                shift
                ;;
            --plan)
                PLAN_ONLY=true
                shift
                ;;
            --destroy)
                DESTROY_MODE=true
                shift
                ;;
            --init-backend)
                INIT_BACKEND_MODE=true
                shift
                ;;
            --help)
                show_usage
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                show_usage
                exit 1
                ;;
        esac
    done
    
    # Execute based on mode
    if [ "$INIT_BACKEND_MODE" = "true" ]; then
        init_backend
        exit 0
    fi
    
    if [ "$DESTROY_MODE" = "true" ]; then
        check_prerequisites
        destroy_infrastructure
        exit 0
    fi
    
    # Standard deployment flow
    check_prerequisites
    validate_terraform
    
    if [ "$VALIDATE_ONLY" = "true" ]; then
        print_status "Validation completed successfully"
        exit 0
    fi
    
    if [ "$PLAN_ONLY" = "true" ]; then
        show_deployment_plan "$FOUNDATION_ONLY"
        exit 0
    fi
    
    # Show plan before deployment
    if ! show_deployment_plan "$FOUNDATION_ONLY"; then
        exit 1
    fi
    
    # Deploy infrastructure
    deploy_infrastructure "$FOUNDATION_ONLY"
    
    # Show outputs and verify
    show_outputs
    verify_deployment
    
    # Final success message
    print_status ""
    print_status "🎉 Meta Kaggle Pipeline deployment completed successfully!"
    print_status ""
    
    if [ "$FOUNDATION_ONLY" = "true" ]; then
        print_info "Foundation deployed. To deploy advanced services:"
        print_info "  1. Set auto_deploy_advanced_services = true in terraform.tfvars"
        print_info "  2. Run: $0 --full-pipeline"
    else
        print_info "Complete pipeline deployed and ready for use!"
        print_info "Next steps:"
        print_info "  1. Upload Meta Kaggle CSV data to S3 raw/ folder"
        print_info "  2. Configure Airflow with deployment outputs"
        print_info "  3. Test ETL pipeline end-to-end"
    fi
    
    print_info ""
    print_info "📊 Monitor costs: AWS Cost Explorer"
    print_info "🔍 View resources: terraform output"
    print_info "📚 Documentation: terraform/README.md"
}

# Run main function with all arguments
main "$@"

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
TERRAFORM_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEV_DIR="$TERRAFORM_DIR/environments/dev"
REGION="us-west-1"
STATE_BUCKET="psi-de-terraform-state"
DYNAMODB_TABLE="terraform-state-locks"

echo -e "${BLUE}🚀 Meta Kaggle Pipeline Infrastructure Deployment${NC}"
echo -e "${BLUE}=================================================${NC}"

# Function to print colored output
print_status() {
    echo -e "${GREEN}✓${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

# Function to check prerequisites
check_prerequisites() {
    print_info "Checking prerequisites..."
    
    # Check AWS CLI
    if ! command -v aws &> /dev/null; then
        print_error "AWS CLI is not installed"
        exit 1
    fi
    
    # Check Terraform
    if ! command -v terraform &> /dev/null; then
        print_error "Terraform is not installed"
        exit 1
    fi
    
    # Check AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        print_error "AWS credentials not configured"
        exit 1
    fi
    
    # Check required environment variables for full deployment
    if [ "$DEPLOYMENT_MODE" = "full" ] || [ "$DEPLOYMENT_MODE" = "advanced" ]; then
        if [ -z "$TF_VAR_redshift_master_password" ]; then
            print_warning "TF_VAR_redshift_master_password not set. Generating secure password..."
            
            # Generate secure password
            GENERATED_PASSWORD=$(generate_redshift_password)
            export TF_VAR_redshift_master_password="$GENERATED_PASSWORD"
            
            print_info "Generated Redshift password. Save this for future reference:"
            print_warning "export TF_VAR_redshift_master_password='$GENERATED_PASSWORD'"
            print_warning "Store this password securely! It won't be shown again."
            
            # Give user time to save the password
            echo ""
            read -p "Press Enter after you've saved the password to continue..." -r
        fi
    fi
    
    print_status "All prerequisites met"
}

# Function to setup Terraform backend
setup_backend() {
    print_info "Setting up Terraform backend..."
    
    # Check if state bucket exists
    if ! aws s3api head-bucket --bucket "$STATE_BUCKET" 2>/dev/null; then
        print_info "Creating Terraform state bucket..."
        aws s3 mb "s3://$STATE_BUCKET" --region "$REGION"
        
        # Enable versioning
        aws s3api put-bucket-versioning \
            --bucket "$STATE_BUCKET" \
            --versioning-configuration Status=Enabled
        
        print_status "State bucket created and versioning enabled"
    else
        print_status "State bucket already exists"
    fi
    
    # Check if DynamoDB table exists
    if ! aws dynamodb describe-table --table-name "$DYNAMODB_TABLE" --region "$REGION" &> /dev/null; then
        print_info "Creating DynamoDB table for state locking..."
        aws dynamodb create-table \
            --table-name "$DYNAMODB_TABLE" \
            --attribute-definitions AttributeName=LockID,AttributeType=S \
            --key-schema AttributeName=LockID,KeyType=HASH \
            --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 \
            --region "$REGION" > /dev/null
        
        # Wait for table to be active
        print_info "Waiting for DynamoDB table to be active..."
        aws dynamodb wait table-exists --table-name "$DYNAMODB_TABLE" --region "$REGION"
        print_status "DynamoDB table created"
    else
        print_status "DynamoDB table already exists"
    fi
}

# Function to deploy infrastructure
deploy_infrastructure() {
    print_info "Deploying infrastructure..."
    
    cd "$DEV_DIR"
    
    # Initialize Terraform
    print_info "Initializing Terraform..."
    terraform init
    print_status "Terraform initialized"
    
    # Validate configuration
    print_info "Validating Terraform configuration..."
    terraform validate
    print_status "Configuration is valid"
    
    # Plan deployment
    print_info "Planning deployment..."
    terraform plan -var-file="terraform.tfvars"
    
    # Confirm deployment
    echo -e "\n${YELLOW}⚠ This will create AWS resources and may incur costs.${NC}"
    echo -e "${BLUE}Continue with deployment? (y/N):${NC}"
    read -r response
    if [[ ! "$response" =~ ^[Yy]$ ]]; then
        print_warning "Deployment cancelled by user"
        exit 0
    fi
    
    # Deploy
    print_info "Deploying infrastructure..."
    if terraform apply -var-file="terraform.tfvars" -auto-approve; then
        print_status "Infrastructure deployed successfully!"
    else
        print_error "Deployment failed"
        exit 1
    fi
}

# Function to display deployment summary
display_summary() {
    print_info "Deployment Summary:"
    echo -e "${BLUE}════════════ OUTPUTS ════════════${NC}"
    terraform output
    echo -e "${BLUE}═════════════════════════════════${NC}"
    
    # Extract key information
    BUCKET_NAME=$(terraform output -raw s3_bucket_name 2>/dev/null || echo "Not available")
    print_info "S3 Data Lake Bucket: $BUCKET_NAME"
    
    # Show cost estimates
    print_info "💰 Expected costs:"
    print_info "  - S3 storage: ~$5-20/month (depending on data volume)"
    print_info "  - Glue crawlers: ~$0.44/hour when running"
    print_info "  - Redshift Serverless: ~$0.375/RPU-hour"
    
    print_info "📊 Next steps:"
    print_info "  1. Upload Meta Kaggle CSV data to s3://$BUCKET_NAME/meta/raw/"
    print_info "  2. Configure Airflow with the output credentials"
    print_info "  3. Run your first ETL pipeline!"
}

# Function to run tests
run_tests() {
    print_info "Running infrastructure tests..."
    
    # Test S3 bucket access
    BUCKET_NAME=$(terraform output -raw s3_bucket_name)
    if aws s3 ls "s3://$BUCKET_NAME" &> /dev/null; then
        print_status "S3 bucket accessible"
    else
        print_error "S3 bucket not accessible"
        return 1
    fi
    
    # Test IAM roles
    GLUE_ROLE_ARN=$(terraform output -raw glue_execution_role_arn)
    if aws iam get-role --role-name "$(basename "$GLUE_ROLE_ARN")" &> /dev/null; then
        print_status "IAM roles accessible"
    else
        print_error "IAM roles not accessible"
        return 1
    fi
    
    print_status "All tests passed"
}

# Main execution
main() {
    print_header
    
    # Parse arguments
    FOUNDATION_ONLY=false
    VALIDATE_ONLY=false
    PLAN_ONLY=false
    DESTROY_MODE=false
    INIT_BACKEND_MODE=false
    DEPLOYMENT_MODE="foundation"  # default to foundation mode
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --foundation-only)
                FOUNDATION_ONLY=true
                DEPLOYMENT_MODE="foundation"
                shift
                ;;
            --full-pipeline)
                FOUNDATION_ONLY=false
                DEPLOYMENT_MODE="full"
                shift
                ;;
            --validate)
                VALIDATE_ONLY=true
                shift
                ;;
            --plan)
                PLAN_ONLY=true
                shift
                ;;
            --destroy)
                DESTROY_MODE=true
                shift
                ;;
            --init-backend)
                INIT_BACKEND_MODE=true
                shift
                ;;
            --help)
                show_usage
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                show_usage
                exit 1
                ;;
        esac
    done
    
    # Execute based on mode
    if [ "$INIT_BACKEND_MODE" = "true" ]; then
        init_backend
        exit 0
    fi
    
    if [ "$DESTROY_MODE" = "true" ]; then
        check_prerequisites
        destroy_infrastructure
        exit 0
    fi
    
    # Standard deployment flow
    check_prerequisites
    validate_terraform
    
    if [ "$VALIDATE_ONLY" = "true" ]; then
        print_status "Validation completed successfully"
        exit 0
    fi
    
    if [ "$PLAN_ONLY" = "true" ]; then
        show_deployment_plan "$FOUNDATION_ONLY"
        exit 0
    fi
    
    # Show plan before deployment
    if ! show_deployment_plan "$FOUNDATION_ONLY"; then
        exit 1
    fi
    
    # Deploy infrastructure
    deploy_infrastructure "$FOUNDATION_ONLY"
    
    # Show outputs and verify
    show_outputs
    verify_deployment
    
    # Final success message
    print_status ""
    print_status "🎉 Meta Kaggle Pipeline deployment completed successfully!"
    print_status ""
    
    if [ "$FOUNDATION_ONLY" = "true" ]; then
        print_info "Foundation deployed. To deploy advanced services:"
        print_info "  1. Set auto_deploy_advanced_services = true in terraform.tfvars"
        print_info "  2. Run: $0 --full-pipeline"
    else
        print_info "Complete pipeline deployed and ready for use!"
        print_info "Next steps:"
        print_info "  1. Upload Meta Kaggle CSV data to S3 raw/ folder"
        print_info "  2. Configure Airflow with deployment outputs"
        print_info "  3. Test ETL pipeline end-to-end"
    fi
    
    print_info ""
    print_info "📊 Monitor costs: AWS Cost Explorer"
    print_info "🔍 View resources: terraform output"
    print_info "📚 Documentation: terraform/README.md"
}

# Run main function with all arguments
main "$@"
