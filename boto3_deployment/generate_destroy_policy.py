#!/usr/bin/env python3
"""
Quick IAM Policy Generator cho Meta Kaggle Pipeline Destruction
Tạo minimal IAM policy để có thể destroy resources
"""

import json

def generate_minimal_destroy_policy():
    """Generate minimal IAM policy for destroy operations"""
    
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "S3Access",
                "Effect": "Allow",
                "Action": [
                    "s3:DeleteBucket",
                    "s3:DeleteObject",
                    "s3:DeleteObjectVersion",
                    "s3:DeleteBucketPolicy",
                    "s3:DeleteBucketLifecycleConfiguration",
                    "s3:ListBucket",
                    "s3:ListBucketVersions",
                    "s3:ListBucketMultipartUploads",
                    "s3:GetBucketLocation",
                    "s3:GetBucketVersioning",
                    "s3:AbortMultipartUpload",
                    "s3:ListMultipartUploadParts"
                ],
                "Resource": [
                    "arn:aws:s3:::psi-de-glue-congdinh",
                    "arn:aws:s3:::psi-de-glue-congdinh/*"
                ]
            },
            {
                "Sid": "IAMAccess",
                "Effect": "Allow", 
                "Action": [
                    "iam:DeleteRole",
                    "iam:DeletePolicy",
                    "iam:DeletePolicyVersion",
                    "iam:ListPolicyVersions",
                    "iam:DetachRolePolicy",
                    "iam:ListAttachedRolePolicies",
                    "iam:ListRolePolicies",
                    "iam:DeleteRolePolicy",
                    "iam:DeleteUser",
                    "iam:DetachUserPolicy",
                    "iam:ListAttachedUserPolicies",
                    "iam:DeleteAccessKey",
                    "iam:ListAccessKeys",
                    "iam:GetUser",
                    "iam:GetRole",
                    "iam:DeleteUserPolicy",
                    "iam:ListUserPolicies",
                    "iam:GetUserPolicy"
                ],
                "Resource": [
                    "arn:aws:iam::*:role/meta-dev-*",
                    "arn:aws:iam::*:policy/meta-dev-*",
                    "arn:aws:iam::*:user/meta-dev-*"
                ]
            },
            {
                "Sid": "GlueAccess",
                "Effect": "Allow",
                "Action": [
                    "glue:DeleteDatabase",
                    "glue:DeleteJob",
                    "glue:DeleteCrawler",
                    "glue:ListCrawlers",
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:GetJob",
                    "glue:GetJobs",
                    "glue:GetCrawler",
                    "glue:GetCrawlers",
                    "glue:DeleteTable",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:BatchDeleteTable"
                ],
                "Resource": "*"
            },
            {
                "Sid": "RedshiftAccess",
                "Effect": "Allow",
                "Action": [
                    "redshift-serverless:DeleteNamespace",
                    "redshift-serverless:DeleteWorkgroup",
                    "redshift-serverless:DeleteUsageLimit",
                    "redshift-serverless:ListUsageLimits"
                ],
                "Resource": "*"
            },
            {
                "Sid": "AthenaAccess",
                "Effect": "Allow",
                "Action": [
                    "athena:DeleteWorkGroup"
                ],
                "Resource": "*"
            },
            {
                "Sid": "STSAccess", 
                "Effect": "Allow",
                "Action": [
                    "sts:GetCallerIdentity"
                ],
                "Resource": "*"
            }
        ]
    }
    
    return policy

def main():
    """Generate and print minimal IAM policy"""
    
    print("="*80)
    print("🔐 MINIMAL IAM POLICY FOR META KAGGLE PIPELINE DESTRUCTION")
    print("="*80)
    
    policy = generate_minimal_destroy_policy()
    
    print("\n📋 Copy this JSON policy and attach to your IAM user:")
    print("\n" + "="*60)
    print(json.dumps(policy, indent=2))
    print("="*60)
    
    print(f"""
🚀 HOW TO APPLY THIS POLICY:

1. AWS Console Method:
   • Go to IAM → Policies → Create Policy
   • Choose JSON tab and paste the policy above
   • Name: MetaKaggleDestroyPolicy
   • Go to IAM → Users → congdinh  
   • Attach Policies → MetaKaggleDestroyPolicy

2. AWS CLI Method:
   • Save the policy JSON to a file: destroy-policy.json
   • Run: aws iam create-policy --policy-name MetaKaggleDestroyPolicy --policy-document file://destroy-policy.json
   • Run: aws iam attach-user-policy --user-name congdinh --policy-arn arn:aws:iam::215103618168:policy/MetaKaggleDestroyPolicy

💡 This policy has minimal permissions needed for resource cleanup.
   For full deployment, use the broader policy from 'python permission_checker.py'

⚠️  IMPORTANT: Replace account ID (215103618168) if different!
""")

if __name__ == "__main__":
    main()