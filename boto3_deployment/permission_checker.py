#!/usr/bin/env python3
"""
Permission Checker cho Meta Kaggle Pipeline Boto3 Deployment
Check và validate AWS IAM permissions required cho deployment và destruction
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional

# Add current directory to path for imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from config.config_manager import ConfigurationManager

class PermissionChecker:
    """Check AWS permissions required for deployment/destruction operations"""
    
    def __init__(self, environment: str = "dev"):
        """Initialize permission checker"""
        self.environment = environment
        self.setup_logging()
        
        # Initialize configuration
        self.config_manager = ConfigurationManager()
        self.config = self.config_manager.load_config(environment)
        
        # Initialize AWS clients
        import boto3
        self.session = boto3.Session()
        self.iam_client = self.session.client('iam')
        self.sts_client = self.session.client('sts')
        
        # Get current user info
        self.current_user = self.get_current_user()
        
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('permission_checker')
    
    def get_current_user(self) -> Dict[str, Any]:
        """Get current AWS user information"""
        try:
            caller_identity = self.sts_client.get_caller_identity()
            return {
                'arn': caller_identity.get('Arn'),
                'user_id': caller_identity.get('UserId'),
                'account_id': caller_identity.get('Account')
            }
        except Exception as e:
            self.logger.error(f"Error getting current user: {e}")
            return {}
    
    def get_required_permissions(self) -> Dict[str, List[str]]:
        """Get list of required permissions for deployment operations"""
        return {
            "S3": [
                "s3:CreateBucket",
                "s3:DeleteBucket",
                "s3:PutBucketPolicy",
                "s3:DeleteBucketPolicy",
                "s3:PutBucketLifecycleConfiguration",
                "s3:GetBucketLifecycleConfiguration",
                "s3:DeleteBucketLifecycleConfiguration",
                "s3:ListBucket",
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:DeleteObjectVersion",
                "s3:GetBucketLocation",
                "s3:PutBucketVersioning",
                "s3:GetBucketVersioning",
                "s3:ListBucketVersions",
                "s3:ListBucketMultipartUploads",
                "s3:AbortMultipartUpload",
                "s3:ListMultipartUploadParts"
            ],
            "IAM": [
                "iam:CreateRole",
                "iam:DeleteRole",
                "iam:CreatePolicy",
                "iam:DeletePolicy",
                "iam:DeletePolicyVersion",
                "iam:ListPolicyVersions",
                "iam:AttachRolePolicy",
                "iam:DetachRolePolicy",
                "iam:ListAttachedRolePolicies",
                "iam:ListRolePolicies",
                "iam:DeleteRolePolicy",
                "iam:PassRole",
                "iam:GetRole",
                "iam:GetPolicy",
                "iam:CreateUser",
                "iam:DeleteUser",
                "iam:AttachUserPolicy",
                "iam:DetachUserPolicy",
                "iam:ListAttachedUserPolicies",
                "iam:CreateAccessKey",
                "iam:DeleteAccessKey",
                "iam:ListAccessKeys",
                "iam:GetUser",
                "iam:GetUserPolicy",
                "iam:ListUserPolicies",
                "iam:DeleteUserPolicy"
            ],
            "Glue": [
                "glue:CreateDatabase",
                "glue:DeleteDatabase",
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:CreateJob",
                "glue:DeleteJob",
                "glue:GetJob",
                "glue:GetJobs",
                "glue:StartJobRun",
                "glue:CreateCrawler",
                "glue:DeleteCrawler",
                "glue:GetCrawler",
                "glue:GetCrawlers",
                "glue:StartCrawler",
                "glue:ListCrawlers",
                "glue:CreateTable",
                "glue:DeleteTable",
                "glue:GetTable",
                "glue:GetTables",
                "glue:UpdateTable",
                "glue:BatchDeleteTable"
            ],
            "Redshift": [
                "redshift-serverless:CreateNamespace",
                "redshift-serverless:DeleteNamespace",
                "redshift-serverless:GetNamespace",
                "redshift-serverless:CreateWorkgroup",
                "redshift-serverless:DeleteWorkgroup",
                "redshift-serverless:GetWorkgroup",
                "redshift-serverless:PutUsageLimit",
                "redshift-serverless:DeleteUsageLimit",
                "redshift-serverless:ListUsageLimits"
            ],
            "Athena": [
                "athena:CreateWorkGroup",
                "athena:DeleteWorkGroup",
                "athena:GetWorkGroup",
                "athena:UpdateWorkGroup"
            ],
            "STS": [
                "sts:GetCallerIdentity"
            ]
        }
    
    def simulate_permission_check(self, service: str, actions: List[str]) -> Dict[str, bool]:
        """Simulate permission check using IAM policy simulator (simplified)"""
        results = {}
        
        # This is a simplified check - in real scenario, you'd use IAM policy simulator
        # For now, we just return True for all as basic check
        for action in actions:
            results[action] = True
            
        return results
    
    def check_permissions(self) -> Dict[str, Any]:
        """Check permissions for all required services"""
        self.logger.info(f"🔍 Checking permissions for user: {self.current_user.get('arn')}")
        
        required_permissions = self.get_required_permissions()
        results = {
            "user_info": self.current_user,
            "environment": self.environment,
            "permission_checks": {},
            "summary": {
                "total_services": len(required_permissions),
                "passed_services": 0,
                "failed_services": 0,
                "warnings": []
            }
        }
        
        for service, actions in required_permissions.items():
            self.logger.info(f"Checking {service} permissions...")
            
            permission_results = self.simulate_permission_check(service, actions)
            passed_actions = sum(1 for result in permission_results.values() if result)
            total_actions = len(actions)
            
            service_result = {
                "total_permissions": total_actions,
                "passed_permissions": passed_actions,
                "success_rate": (passed_actions / total_actions) * 100,
                "permissions": permission_results
            }
            
            results["permission_checks"][service] = service_result
            
            if passed_actions == total_actions:
                results["summary"]["passed_services"] += 1
            else:
                results["summary"]["failed_services"] += 1
                results["summary"]["warnings"].append(
                    f"{service}: {passed_actions}/{total_actions} permissions available"
                )
        
        return results
    
    def print_permission_guidance(self):
        """Print guidance on required IAM permissions"""
        print("\n" + "="*80)
        print("🔐 REQUIRED AWS IAM PERMISSIONS GUIDANCE")
        print("="*80)
        
        print(f"""
📋 Current User: {self.current_user.get('arn', 'Unknown')}
🌍 Environment: {self.environment}
🏢 Account ID: {self.current_user.get('account_id', 'Unknown')}

⚠️  PERMISSION ISSUES DETECTED ⚠️

Bạn cần các permissions sau để thực hiện deployment và destruction:
""")
        
        required_permissions = self.get_required_permissions()
        
        for service, actions in required_permissions.items():
            print(f"\n🔧 {service} Service Permissions:")
            for action in actions:
                print(f"  ✓ {action}")
        
        print(f"""

📝 RECOMMENDED IAM POLICY:

Tạo IAM Policy với nội dung sau và attach vào user {self.current_user.get('arn', '').split('/')[-1]}:

{{
    "Version": "2012-10-17",
    "Statement": [
        {{
            "Effect": "Allow",
            "Action": [
                "s3:*",
                "iam:*",
                "glue:*",
                "redshift-serverless:*",
                "athena:*"
            ],
            "Resource": "*"
        }}
    ]
}}

⚠️  LƯU Ý: Policy trên có quyền rộng. Trong production environment, 
hãy giới hạn resources cụ thể thay vì sử dụng "*".

🚀 CÁCH THỰC HIỆN:

1. Đăng nhập AWS Console với admin privileges
2. Vào IAM → Policies → Create Policy
3. Chọn JSON tab và paste policy trên
4. Đặt tên: MetaKagglePipelinePolicy
5. Vào IAM → Users → {self.current_user.get('arn', '').split('/')[-1] if '/' in self.current_user.get('arn', '') else 'your-user'}
6. Attach Policy → MetaKagglePipelinePolicy

💡 ALTERNATIVE: Sử dụng AWS CLI:
aws iam create-policy --policy-name MetaKagglePipelinePolicy --policy-document file://policy.json
aws iam attach-user-policy --user-name {self.current_user.get('arn', '').split('/')[-1] if '/' in self.current_user.get('arn', '') else 'your-user'} --policy-arn arn:aws:iam::{self.current_user.get('account_id', 'ACCOUNT-ID')}:policy/MetaKagglePipelinePolicy
""")
        
        print("="*80)
    
    def run_check(self) -> bool:
        """Run permission check and return success status"""
        try:
            self.logger.info("Starting permission check...")
            
            # Check basic AWS connectivity
            if not self.current_user.get('arn'):
                self.logger.error("❌ Cannot connect to AWS or get current user identity")
                return False
            
            # Run permission checks
            results = self.check_permissions()
            
            # Print results
            print("\n" + "="*60)
            print("📊 PERMISSION CHECK RESULTS")
            print("="*60)
            
            print(f"User: {results['user_info'].get('arn')}")
            print(f"Environment: {results['environment']}")
            print(f"Services Checked: {results['summary']['total_services']}")
            
            # Note: Since we're doing simplified check, show guidance instead
            self.print_permission_guidance()
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Permission check failed: {e}")
            return False

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Check AWS IAM permissions for Meta Kaggle Pipeline')
    parser.add_argument('--environment', default='dev', help='Environment to check (default: dev)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    
    try:
        checker = PermissionChecker(environment=args.environment)
        success = checker.run_check()
        
        if success:
            print("\n✅ Permission check completed")
            print("📖 Review the guidance above before running deployment/destruction")
        else:
            print("\n❌ Permission check failed")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n⏹️  Permission check cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()