#!/usr/bin/env python3
"""
IAMManager cho Meta Kaggle Pipeline
Tạo IAM roles, policies và auto-grant permissions
Tương tự terraform/modules/iam/main.tf
"""

import json
from typing import Dict, List, Any, Optional
from botocore.exceptions import ClientError

from .base_manager import BaseInfrastructureManager


class IAMManager(BaseInfrastructureManager):
    """
    Manager cho IAM resources
    Tương tự terraform/modules/iam/
    """
    
    def __init__(self, config: Dict[str, Any], session=None):
        """Khởi tạo IAMManager"""
        super().__init__(config, session)
        self.iam_client = self.get_client('iam')
        self.sts_client = self.get_client('sts')
        
        self.s3_bucket_name = config['s3_bucket_name']
        self.s3_bucket_arn = f"arn:aws:s3:::{self.s3_bucket_name}"
        
        # Get current user info for auto-granting permissions
        self.current_user_info = self._get_current_user_info()
    
    def validate_config(self) -> bool:
        """Validate IAM configuration"""
        if not self.s3_bucket_name:
            raise ValueError("S3 bucket name is required for IAM policies")
        return True
    
    def _get_current_user_info(self) -> Dict[str, str]:
        """Get current AWS user information"""
        try:
            response = self.sts_client.get_caller_identity()
            arn = response['Arn']
            
            # Extract username from ARN (format: arn:aws:iam::account:user/username)
            if ':user/' in arn:
                username = arn.split(':user/')[1]
            else:
                # Might be a role or other identity
                username = arn.split('/')[-1]
            
            return {
                'arn': arn,
                'username': username,
                'account_id': response['Account'],
                'user_id': response['UserId']
            }
        except Exception as e:
            self.logger.warning(f"Could not get current user info: {e}")
            return {}
    
    def create_glue_execution_role(self) -> Dict[str, str]:
        """
        Tạo Glue execution role
        Tương tự aws_iam_role.glue_execution_role trong terraform
        """
        role_name = self.format_resource_name("glue-execution", "role")
        
        # Trust policy cho Glue service
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "glue.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }]
        }
        
        try:
            # Check if role exists
            try:
                response = self.iam_client.get_role(RoleName=role_name)
                self.logger.info(f"Glue role {role_name} already exists")
                role_arn = response['Role']['Arn']
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchEntity':
                    # Create new role
                    response = self.iam_client.create_role(
                        RoleName=role_name,
                        AssumeRolePolicyDocument=json.dumps(trust_policy),
                        Description="Execution role for Glue jobs in Meta Kaggle Pipeline",
                        Tags=[
                            {'Key': k, 'Value': v} 
                            for k, v in self.add_default_tags({
                                'Service': 'Glue',
                                'Purpose': 'execution-role'
                            }).items()
                        ]
                    )
                    role_arn = response['Role']['Arn']
                    self.logger.info(f"Created Glue execution role: {role_name}")
                else:
                    raise e
            
            # Attach AWS managed policy
            self._attach_managed_policy_to_role(
                role_name, 
                "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
            )
            
            # Create và attach custom S3 policy
            s3_policy_arn = self._create_glue_s3_policy()
            self._attach_policy_to_role(role_name, s3_policy_arn)
            
            return {
                "role_name": role_name,
                "role_arn": role_arn
            }
            
        except ClientError as e:
            self.handle_aws_error(e, f"creating Glue execution role {role_name}")
    
    def _create_glue_s3_policy(self) -> str:
        """Tạo custom S3 policy cho Glue"""
        policy_name = self.format_resource_name("glue-s3-access", "policy")
        
        # S3 permissions policy for Glue
        policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject",
                        "s3:ListBucket"
                    ],
                    "Resource": [
                        self.s3_bucket_arn,
                        f"{self.s3_bucket_arn}/*"
                    ]
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetBucketLocation",
                        "s3:ListBucket",
                        "s3:ListBucketMultipartUploads"
                    ],
                    "Resource": self.s3_bucket_arn
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream", 
                        "logs:PutLogEvents"
                    ],
                    "Resource": f"arn:aws:logs:{self.aws_region}:*:*"
                }
            ]
        }
        
        return self._create_policy(policy_name, policy_document, "S3 access policy for Glue jobs")
    
    def create_redshift_service_role(self) -> Dict[str, str]:
        """
        Tạo Redshift service role
        Tương tự aws_iam_role.redshift_service_role
        """
        role_name = self.format_resource_name("redshift-service", "role")
        
        # Trust policy cho Redshift
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "redshift.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }]
        }
        
        try:
            # Check if role exists
            try:
                response = self.iam_client.get_role(RoleName=role_name)
                self.logger.info(f"Redshift role {role_name} already exists")
                role_arn = response['Role']['Arn']
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchEntity':
                    # Create new role
                    response = self.iam_client.create_role(
                        RoleName=role_name,
                        AssumeRolePolicyDocument=json.dumps(trust_policy),
                        Description="Service role for Redshift in Meta Kaggle Pipeline",
                        Tags=[
                            {'Key': k, 'Value': v} 
                            for k, v in self.add_default_tags({
                                'Service': 'Redshift',
                                'Purpose': 'service-role'
                            }).items()
                        ]
                    )
                    role_arn = response['Role']['Arn']
                    self.logger.info(f"Created Redshift service role: {role_name}")
                else:
                    raise e
            
            # Create và attach custom S3 policy for COPY operations
            s3_policy_arn = self._create_redshift_s3_policy()
            self._attach_policy_to_role(role_name, s3_policy_arn)
            
            return {
                "role_name": role_name,
                "role_arn": role_arn
            }
            
        except ClientError as e:
            self.handle_aws_error(e, f"creating Redshift service role {role_name}")
    
    def _create_redshift_s3_policy(self) -> str:
        """Tạo S3 policy cho Redshift COPY operations"""
        policy_name = self.format_resource_name("redshift-s3-copy", "policy")
        
        policy_document = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    self.s3_bucket_arn,
                    f"{self.s3_bucket_arn}/*"
                ]
            }]
        }
        
        return self._create_policy(policy_name, policy_document, "S3 access policy for Redshift COPY")
    
    def create_airflow_user(self) -> Dict[str, str]:
        """
        Tạo Airflow IAM user với access keys
        Tương tự aws_iam_user.airflow_user
        """
        user_name = self.format_resource_name("airflow", "user")
        
        try:
            # Check if user exists
            try:
                response = self.iam_client.get_user(UserName=user_name)
                self.logger.info(f"Airflow user {user_name} already exists")
                user_arn = response['User']['Arn']
                access_key_id = None
                secret_access_key = None
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchEntity':
                    # Create new user
                    response = self.iam_client.create_user(
                        UserName=user_name,
                        Tags=[
                            {'Key': k, 'Value': v} 
                            for k, v in self.add_default_tags({
                                'Service': 'Airflow',
                                'Purpose': 'orchestration-user'
                            }).items()
                        ]
                    )
                    user_arn = response['User']['Arn']
                    self.logger.info(f"Created Airflow user: {user_name}")
                    
                    # Create access key
                    access_key_response = self.iam_client.create_access_key(UserName=user_name)
                    access_key_id = access_key_response['AccessKey']['AccessKeyId']
                    secret_access_key = access_key_response['AccessKey']['SecretAccessKey']
                    
                else:
                    raise e
            
            # Create và attach Airflow policy
            airflow_policy_arn = self._create_airflow_policy()
            self._attach_policy_to_user(user_name, airflow_policy_arn)
            
            return {
                "user_name": user_name,
                "user_arn": user_arn,
                "access_key_id": access_key_id,
                "secret_access_key": secret_access_key
            }
            
        except ClientError as e:
            self.handle_aws_error(e, f"creating Airflow user {user_name}")
    
    def _create_airflow_policy(self) -> str:
        """Tạo policy cho Airflow user"""
        policy_name = self.format_resource_name("airflow-orchestration", "policy")
        
        policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject",
                        "s3:ListBucket"
                    ],
                    "Resource": [
                        self.s3_bucket_arn,
                        f"{self.s3_bucket_arn}/*"
                    ]
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "glue:StartJobRun",
                        "glue:GetJobRun",
                        "glue:GetJobRuns",
                        "glue:BatchStopJobRun",
                        "glue:StartCrawler",
                        "glue:GetCrawler",
                        "glue:GetCrawlerMetrics",
                        "glue:StopCrawler"
                    ],
                    "Resource": "*"
                },
                {
                    "Effect": "Allow", 
                    "Action": [
                        "athena:StartQueryExecution",
                        "athena:GetQueryExecution",
                        "athena:GetQueryResults",
                        "athena:StopQueryExecution"
                    ],
                    "Resource": "*"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "redshift-serverless:ExecuteStatement",
                        "redshift-serverless:DescribeStatement",
                        "redshift-serverless:ListStatements"
                    ],
                    "Resource": "*"
                }
            ]
        }
        
        return self._create_policy(policy_name, policy_document, "Orchestration policy for Airflow")
    
    def create_meta_kaggle_pipeline_policy(self) -> str:
        """
        Tạo Meta Kaggle Pipeline policy và auto-attach vào current user
        Tương tự logic trong terraform IAM module
        """
        policy_name = self.format_resource_name("meta-kaggle-pipeline", "policy")
        
        # Include Secrets Manager permissions nếu enabled
        secrets_manager_actions = []
        if self.config.get('enable_secrets_manager', False):
            secrets_manager_actions = [
                "secretsmanager:GetSecretValue",
                "secretsmanager:DescribeSecret"
            ]
        
        policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:ListBucket",
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject",
                        "s3:GetBucketLocation"
                    ],
                    "Resource": [
                        self.s3_bucket_arn,
                        f"{self.s3_bucket_arn}/*"
                    ]
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "glue:*Database",
                        "glue:*Table",
                        "glue:*Crawler",
                        "glue:*Job",
                        "glue:GetTables",
                        "glue:GetTable",
                        "glue:GetPartitions"
                    ],
                    "Resource": "*"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "athena:StartQueryExecution",
                        "athena:GetQueryExecution", 
                        "athena:GetQueryResults",
                        "athena:ListQueryExecutions",
                        "athena:ListWorkGroups",
                        "athena:GetWorkGroup",
                        "athena:CreateWorkGroup",
                        "athena:UpdateWorkGroup",
                        "athena:DeleteWorkGroup",
                        "athena:TagResource",
                        "athena:UntagResource",
                        "athena:ListTagsForResource"
                    ],
                    "Resource": "*"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "redshift-serverless:GetNamespace",
                        "redshift-serverless:GetWorkgroup",
                        "redshift-serverless:ListNamespaces",
                        "redshift-serverless:ListWorkgroups",
                        "redshift-serverless:CreateNamespace",
                        "redshift-serverless:CreateWorkgroup",
                        "redshift-serverless:UpdateNamespace",
                        "redshift-serverless:UpdateWorkgroup",
                        "redshift-serverless:DeleteNamespace",
                        "redshift-serverless:DeleteWorkgroup",
                        "redshift-serverless:TagResource",
                        "redshift-serverless:UntagResource",
                        "redshift-serverless:ListTagsForResource",
                        "redshift-serverless:CreateUsageLimit",
                        "redshift-serverless:UpdateUsageLimit",
                        "redshift-serverless:DeleteUsageLimit",
                        "redshift-serverless:GetUsageLimit",
                        "redshift-serverless:ListUsageLimits",
                        "redshift-serverless:ExecuteStatement",
                        "redshift-serverless:DescribeStatement"
                    ],
                    "Resource": "*"
                },
                {
                    "Effect": "Allow", 
                    "Action": [
                        "iam:PassRole"
                    ],
                    "Resource": [
                        f"arn:aws:iam::*:role/{self.format_resource_name('glue-execution', 'role')}",
                        f"arn:aws:iam::*:role/{self.format_resource_name('redshift-service', 'role')}"
                    ]
                }
            ]
        }
        
        # Add Secrets Manager actions if enabled
        if secrets_manager_actions:
            policy_document["Statement"].append({
                "Effect": "Allow",
                "Action": secrets_manager_actions,
                "Resource": f"arn:aws:secretsmanager:{self.aws_region}:*:secret:meta-*"
            })
        
        policy_arn = self._create_policy(
            policy_name, policy_document, 
            "Meta Kaggle Pipeline comprehensive access policy"
        )
        
        # Auto-attach to current user
        if self.current_user_info.get('username'):
            try:
                self._attach_policy_to_user(self.current_user_info['username'], policy_arn)
                self.logger.info(
                    f"Auto-attached Meta Kaggle Pipeline policy to user: {self.current_user_info['username']}"
                )
            except Exception as e:
                self.logger.warning(f"Could not auto-attach policy to current user: {e}")
        
        return policy_arn
    
    def _create_policy(self, policy_name: str, policy_document: Dict, description: str) -> str:
        """Helper để tạo IAM policy"""
        try:
            # Check if policy exists
            policy_arn = f"arn:aws:iam::{self.current_user_info.get('account_id', '000000000000')}:policy/{policy_name}"
            
            try:
                self.iam_client.get_policy(PolicyArn=policy_arn)
                # Policy exists, update it with new permissions
                self.logger.info(f"Policy {policy_name} already exists - updating with new permissions")
                
                # Create new policy version
                response = self.iam_client.create_policy_version(
                    PolicyArn=policy_arn,
                    PolicyDocument=json.dumps(policy_document),
                    SetAsDefault=True
                )
                self.logger.info(f"Updated policy {policy_name} with new version")
                return policy_arn
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchEntity':
                    # Create new policy
                    response = self.iam_client.create_policy(
                        PolicyName=policy_name,
                        PolicyDocument=json.dumps(policy_document),
                        Description=description,
                        Tags=[
                            {'Key': k, 'Value': v} 
                            for k, v in self.add_default_tags().items()
                        ]
                    )
                    policy_arn = response['Policy']['Arn']
                    self.logger.info(f"Created policy: {policy_name}")
                    return policy_arn
                else:
                    raise e
        except ClientError as e:
            self.handle_aws_error(e, f"creating policy {policy_name}")
    
    def _attach_managed_policy_to_role(self, role_name: str, policy_arn: str) -> None:
        """Attach managed policy to role"""
        try:
            self.iam_client.attach_role_policy(
                RoleName=role_name,
                PolicyArn=policy_arn
            )
            self.logger.debug(f"Attached managed policy {policy_arn} to role {role_name}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityAlreadyExists':
                self.logger.debug(f"Policy {policy_arn} already attached to role {role_name}")
            else:
                self.handle_aws_error(e, f"attaching managed policy to role {role_name}")
    
    def _attach_policy_to_role(self, role_name: str, policy_arn: str) -> None:
        """Attach custom policy to role"""
        try:
            self.iam_client.attach_role_policy(
                RoleName=role_name,
                PolicyArn=policy_arn
            )
            self.logger.debug(f"Attached policy {policy_arn} to role {role_name}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityAlreadyExists':
                self.logger.debug(f"Policy {policy_arn} already attached to role {role_name}")
            else:
                self.handle_aws_error(e, f"attaching policy to role {role_name}")
    
    def _attach_policy_to_user(self, user_name: str, policy_arn: str) -> None:
        """Attach policy to user"""
        try:
            self.iam_client.attach_user_policy(
                UserName=user_name,
                PolicyArn=policy_arn
            )
            self.logger.debug(f"Attached policy {policy_arn} to user {user_name}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityAlreadyExists':
                self.logger.debug(f"Policy {policy_arn} already attached to user {user_name}")
            else:
                self.handle_aws_error(e, f"attaching policy to user {user_name}")
    
    def deploy(self) -> Dict[str, Any]:
        """Deploy complete IAM infrastructure"""
        self.logger.info("Starting IAM deployment")
        results = {}
        
        try:
            # 1. Create Glue execution role
            glue_role = self.create_glue_execution_role()
            results['glue_role'] = glue_role
            
            # 2. Create Redshift service role
            redshift_role = self.create_redshift_service_role()
            results['redshift_role'] = redshift_role
            
            # 3. Create Airflow user
            airflow_user = self.create_airflow_user()
            results['airflow_user'] = airflow_user
            
            # 4. Create Meta Kaggle Pipeline policy và auto-attach
            meta_policy_arn = self.create_meta_kaggle_pipeline_policy()
            results['meta_kaggle_pipeline_policy'] = {
                'policy_arn': meta_policy_arn,
                'attached_to_user': self.current_user_info.get('username')
            }
            
            results['deployment_status'] = 'success'
            results['current_user_info'] = self.current_user_info
            
            self.logger.info("IAM deployment completed successfully")
            return results
            
        except Exception as e:
            self.logger.error(f"IAM deployment failed: {e}")
            results['deployment_status'] = 'failed'
            results['error'] = str(e)
            return results
    
    def get_deployment_info(self) -> Dict[str, Any]:
        """Get deployment information"""
        return {
            'service': 'IAM',
            'current_user': self.current_user_info,
            'roles': {
                'glue_execution_role': self.format_resource_name("glue-execution", "role"),
                'redshift_service_role': self.format_resource_name("redshift-service", "role")
            },
            'users': {
                'airflow_user': self.format_resource_name("airflow", "user")
            },
            'policies': {
                'meta_kaggle_pipeline_policy': self.format_resource_name("meta-kaggle-pipeline", "policy")
            },
            'region': self.aws_region,
            'environment': self.environment
        }
    
    def resource_exists(self, resource_identifier: str) -> bool:
        """Check if IAM resource exists"""
        # This would need specific implementation based on resource type
        return False
    
    def _detach_policy_from_user(self, policy_arn, username):
        """Detach policy from user with error handling"""
        try:
            self.iam_client.detach_user_policy(
                UserName=username,
                PolicyArn=policy_arn
            )
            self.logger.info(f"✓ Detached policy from user: {username}")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchEntity':
                self.logger.warning(f"Policy or user not found when detaching: {policy_arn} from {username}")
            else:
                self.logger.error(f"Error detaching policy from user: {e}")
                raise

    def _delete_policy_versions(self, policy_arn):
        """Delete all non-default versions of a policy before deletion"""
        try:
            # List all policy versions
            versions = self.iam_client.list_policy_versions(PolicyArn=policy_arn)
            
            # Delete all non-default versions
            for version in versions['Versions']:
                if not version['IsDefaultVersion']:
                    try:
                        self.iam_client.delete_policy_version(
                            PolicyArn=policy_arn,
                            VersionId=version['VersionId']
                        )
                        self.logger.info(f"✓ Deleted policy version: {version['VersionId']}")
                    except ClientError as e:
                        self.logger.warning(f"Could not delete policy version {version['VersionId']}: {e}")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchEntity':
                self.logger.info(f"Policy {policy_arn} not found when listing versions (may already be deleted)")
            else:
                self.logger.warning(f"Could not list policy versions for {policy_arn}: {e}")

    def create_and_attach_destroy_policy(self) -> Dict[str, Any]:
        """Create comprehensive destroy policy and attach to current user"""
        try:
            current_user_info = self._get_current_user_info()
            username = current_user_info.get('username')
            account_id = current_user_info.get('account_id')
            
            if not username or not account_id:
                self.logger.error("Could not get current user info for policy attachment")
                return {"success": False, "error": "Missing user info"}
            
            # Create comprehensive destroy policy
            destroy_policy_name = f"{self.project_name}-{self.environment}-destroy-policy"
            destroy_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "S3DestroyAccess",
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
                            f"arn:aws:s3:::{self.project_name}-*",
                            f"arn:aws:s3:::{self.project_name}-*/*"
                        ]
                    },
                    {
                        "Sid": "IAMDestroyAccess",
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
                            f"arn:aws:iam::{account_id}:role/{self.project_name}-{self.environment}-*",
                            f"arn:aws:iam::{account_id}:policy/{self.project_name}-{self.environment}-*",
                            f"arn:aws:iam::{account_id}:user/{self.project_name}-{self.environment}-*"
                        ]
                    },
                    {
                        "Sid": "GlueDestroyAccess",
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
                        "Resource": "*",
                        "Condition": {
                            "StringLike": {
                                "glue:DatabaseName": f"{self.project_name.replace('-', '_')}*"
                            }
                        }
                    },
                    {
                        "Sid": "RedshiftDestroyAccess",
                        "Effect": "Allow",
                        "Action": [
                            "redshift-serverless:DeleteNamespace",
                            "redshift-serverless:DeleteWorkgroup", 
                            "redshift-serverless:DeleteUsageLimit",
                            "redshift-serverless:ListUsageLimits",
                            "redshift-serverless:GetNamespace",
                            "redshift-serverless:GetWorkgroup"
                        ],
                        "Resource": "*",
                        "Condition": {
                            "StringLike": {
                                "aws:RequestedRegion": self.aws_region
                            }
                        }
                    },
                    {
                        "Sid": "AthenaDestroyAccess",
                        "Effect": "Allow",
                        "Action": [
                            "athena:DeleteWorkGroup",
                            "athena:GetWorkGroup"
                        ],
                        "Resource": f"arn:aws:athena:{self.aws_region}:{account_id}:workgroup/{self.project_name}-{self.environment}-*"
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
            
            # Create the policy
            policy_arn = self._create_policy(
                destroy_policy_name,
                destroy_policy,
                f"Auto-generated destroy policy for {self.project_name} {self.environment}"
            )
            
            # Attach policy to current user
            try:
                self.iam_client.attach_user_policy(
                    UserName=username,
                    PolicyArn=policy_arn
                )
                
                self.logger.info(f"✅ Auto-granted destroy permissions to user: {username}")
                
                return {
                    "success": True,
                    "policy_name": destroy_policy_name,
                    "policy_arn": policy_arn,
                    "attached_to": username
                }
                
            except ClientError as e:
                self.logger.error(f"Failed to attach destroy policy to user {username}: {e}")
                return {"success": False, "error": f"Policy attachment failed: {str(e)}"}
                
        except Exception as e:
            self.logger.error(f"Failed to create and attach destroy policy: {e}")
            return {"success": False, "error": str(e)}