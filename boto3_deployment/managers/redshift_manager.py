#!/usr/bin/env python3
"""
RedshiftManager cho Meta Kaggle Pipeline  
Tạo Redshift Serverless namespace, workgroup và usage limits
Tương tự terraform/modules/redshift/main.tf
"""

import time
from typing import Dict, List, Any, Optional
from botocore.exceptions import ClientError

from .base_manager import BaseInfrastructureManager


class RedshiftManager(BaseInfrastructureManager):
    """
    Manager cho Redshift Serverless
    Tương tự terraform/modules/redshift/
    """
    
    def __init__(self, config: Dict[str, Any], session=None):
        """Khởi tạo RedshiftManager"""
        super().__init__(config, session)
        self.redshift_serverless_client = self.get_client('redshift-serverless')
        
        # Redshift configuration
        self.database_name = config.get('redshift_database_name', 'meta_warehouse')
        self.master_username = config.get('redshift_master_username', 'meta_admin')
        self.master_password = config.get('redshift_master_password')
        
        # Capacity configuration
        self.base_capacity_rpus = config.get('redshift_base_capacity_rpus', 8)
        self.max_capacity_rpus = config.get('redshift_max_capacity_rpus', 64)
        
        # Network configuration
        self.publicly_accessible = config.get('redshift_publicly_accessible', False)
        self.subnet_ids = config.get('private_subnet_ids', [])
        self.security_group_ids = config.get('redshift_security_group_ids', [])
        
        # Usage limits
        self.enable_usage_limits = config.get('enable_redshift_usage_limits', True)
        self.monthly_usage_limit = config.get('redshift_monthly_usage_limit', 500)
        self.weekly_usage_limit = config.get('redshift_weekly_usage_limit', 125)
        self.usage_limit_breach_action = config.get('redshift_usage_limit_breach_action', 'log')
        
        # Resource names
        self.namespace_name = f"meta-{self.environment}-namespace"
        self.workgroup_name = f"meta-{self.environment}-workgroup"
        
        # IAM role ARN (sẽ được set từ IAM deployment)
        self.redshift_role_arn = None
    
    def set_redshift_role_arn(self, role_arn: str) -> None:
        """Set Redshift role ARN từ IAM deployment"""
        self.redshift_role_arn = role_arn
    
    def validate_config(self) -> bool:
        """Validate Redshift configuration"""
        if not self.master_password:
            raise ValueError("Redshift master password is required")
        
        if len(self.master_password) < 8 or len(self.master_password) > 64:
            raise ValueError("Redshift password must be 8-64 characters")
        
        if self.base_capacity_rpus < 8:
            raise ValueError("Redshift base capacity must be at least 8 RPUs")
        
        if self.max_capacity_rpus < self.base_capacity_rpus:
            raise ValueError("Redshift max capacity must be >= base capacity")
        
        if not self.redshift_role_arn:
            self.logger.warning("Redshift role ARN not set - will need to be provided during deployment")
        
        return True
    
    def create_namespace(self) -> Dict[str, str]:
        """
        Tạo Redshift Serverless namespace
        Tương tự aws_redshiftserverless_namespace resource
        """
        try:
            # Check if namespace exists
            try:
                response = self.redshift_serverless_client.get_namespace(
                    namespaceName=self.namespace_name
                )
                self.logger.info(f"Namespace {self.namespace_name} already exists")
                namespace_info = response['namespace']
                return {
                    "namespace_name": namespace_info['namespaceName'],
                    "namespace_arn": namespace_info['namespaceArn'],
                    "namespace_id": namespace_info['namespaceId'],
                    "created": False
                }
            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceNotFoundException':
                    # Create new namespace
                    response = self.redshift_serverless_client.create_namespace(
                        namespaceName=self.namespace_name,
                        dbName=self.database_name,
                        adminUsername=self.master_username,
                        adminUserPassword=self.master_password,
                        iamRoles=[self.redshift_role_arn] if self.redshift_role_arn else [],
                        logExports=['userlog', 'connectionlog', 'useractivitylog'],
                        tags=[
                            {'key': k, 'value': v} 
                            for k, v in self.add_default_tags({
                                'Service': 'RedshiftServerless',
                                'Component': 'namespace',
                                'Purpose': 'serverless-namespace'
                            }).items()
                        ]
                    )
                    
                    namespace_info = response['namespace']
                    self.logger.info(f"Created Redshift namespace: {self.namespace_name}")
                    
                    # Wait for namespace to be available
                    self._wait_for_namespace_available()
                    
                    return {
                        "namespace_name": namespace_info['namespaceName'],
                        "namespace_arn": namespace_info['namespaceArn'],
                        "namespace_id": namespace_info['namespaceId'],
                        "created": True
                    }
                else:
                    raise e
                    
        except ClientError as e:
            self.handle_aws_error(e, f"creating namespace {self.namespace_name}")
    
    def _wait_for_namespace_available(self, max_attempts: int = 30) -> bool:
        """Wait for namespace to be available"""
        self.logger.info(f"Waiting for namespace {self.namespace_name} to be available...")
        
        def check_namespace_status():
            try:
                response = self.redshift_serverless_client.get_namespace(
                    namespaceName=self.namespace_name
                )
                status = response['namespace']['status']
                self.logger.debug(f"Namespace status: {status}")
                return status == 'AVAILABLE'
            except Exception:
                return False
        
        return self.wait_for_resource(check_namespace_status, max_attempts, 20)
    
    def create_workgroup(self) -> Dict[str, str]:
        """
        Tạo Redshift Serverless workgroup
        Tương tự aws_redshiftserverless_workgroup resource
        """
        try:
            # Check if workgroup exists
            try:
                response = self.redshift_serverless_client.get_workgroup(
                    workgroupName=self.workgroup_name
                )
                self.logger.info(f"Workgroup {self.workgroup_name} already exists")
                workgroup_info = response['workgroup']
                return {
                    "workgroup_name": workgroup_info['workgroupName'],
                    "workgroup_arn": workgroup_info['workgroupArn'],
                    "workgroup_id": workgroup_info['workgroupId'],
                    "endpoint": workgroup_info.get('endpoint', {}),
                    "created": False
                }
            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceNotFoundException':
                    # Create new workgroup
                    create_params = {
                        'workgroupName': self.workgroup_name,
                        'namespaceName': self.namespace_name,
                        'baseCapacity': self.base_capacity_rpus,
                        'maxCapacity': self.max_capacity_rpus,
                        'publiclyAccessible': self.publicly_accessible,
                        'tags': [
                            {'key': k, 'value': v} 
                            for k, v in self.add_default_tags({
                                'Service': 'RedshiftServerless',
                                'Component': 'workgroup',
                                'BaseCapacity': f"{self.base_capacity_rpus}RPUs",
                                'MaxCapacity': f"{self.max_capacity_rpus}RPUs"
                            }).items()
                        ]
                    }
                    
                    # Add VPC configuration only if subnet IDs are provided
                    if self.subnet_ids:
                        create_params['subnetIds'] = self.subnet_ids
                        create_params['enhancedVpcRouting'] = True
                        
                        if self.security_group_ids:
                            create_params['securityGroupIds'] = self.security_group_ids
                    
                    response = self.redshift_serverless_client.create_workgroup(**create_params)
                    
                    workgroup_info = response['workgroup']
                    self.logger.info(f"Created Redshift workgroup: {self.workgroup_name}")
                    
                    # Wait for workgroup to be available
                    self._wait_for_workgroup_available()
                    
                    # Get updated workgroup info with endpoint
                    updated_response = self.redshift_serverless_client.get_workgroup(
                        workgroupName=self.workgroup_name
                    )
                    workgroup_info = updated_response['workgroup']
                    
                    return {
                        "workgroup_name": workgroup_info['workgroupName'],
                        "workgroup_arn": workgroup_info['workgroupArn'],
                        "workgroup_id": workgroup_info['workgroupId'],
                        "endpoint": workgroup_info.get('endpoint', {}),
                        "created": True
                    }
                else:
                    raise e
                    
        except ClientError as e:
            self.handle_aws_error(e, f"creating workgroup {self.workgroup_name}")
    
    def _wait_for_workgroup_available(self, max_attempts: int = 30) -> bool:
        """Wait for workgroup to be available"""
        self.logger.info(f"Waiting for workgroup {self.workgroup_name} to be available...")
        
        def check_workgroup_status():
            try:
                response = self.redshift_serverless_client.get_workgroup(
                    workgroupName=self.workgroup_name
                )
                status = response['workgroup']['status']
                self.logger.debug(f"Workgroup status: {status}")
                return status == 'AVAILABLE'
            except Exception:
                return False
        
        return self.wait_for_resource(check_workgroup_status, max_attempts, 20)
    
    def create_usage_limits(self) -> Dict[str, List[str]]:
        """
        Tạo usage limits cho cost control
        Tương tự aws_redshiftserverless_usage_limit resources
        """
        if not self.enable_usage_limits:
            self.logger.info("Usage limits disabled, skipping")
            return {"usage_limits": []}
        
        created_limits = []
        
        # Get workgroup ARN for usage limits
        workgroup_response = self.redshift_serverless_client.get_workgroup(
            workgroupName=self.workgroup_name
        )
        workgroup_arn = workgroup_response['workgroup']['workgroupArn']
        
        # Monthly usage limit
        try:
            response = self.redshift_serverless_client.create_usage_limit(
                resourceArn=workgroup_arn,
                usageType='serverless-compute',
                amount=self.monthly_usage_limit,
                period='monthly',
                breachAction=self.usage_limit_breach_action
            )
            
            created_limits.append({
                'usage_limit_id': response['usageLimit']['usageLimitId'],
                'period': 'monthly',
                'amount': self.monthly_usage_limit
            })
            self.logger.info(f"Created monthly usage limit: {self.monthly_usage_limit} RPU-hours")
            
        except ClientError as e:
            self.logger.error(f"Failed to create monthly usage limit: {e}")
        
        # Weekly usage limit
        try:
            response = self.redshift_serverless_client.create_usage_limit(
                resourceArn=workgroup_arn,
                usageType='serverless-compute',
                amount=self.weekly_usage_limit,
                period='weekly',
                breachAction=self.usage_limit_breach_action
            )
            
            created_limits.append({
                'usage_limit_id': response['usageLimit']['usageLimitId'],
                'period': 'weekly',
                'amount': self.weekly_usage_limit
            })
            self.logger.info(f"Created weekly usage limit: {self.weekly_usage_limit} RPU-hours")
            
        except ClientError as e:
            self.logger.error(f"Failed to create weekly usage limit: {e}")
        
        return {"usage_limits": created_limits}
    
    def deploy(self) -> Dict[str, Any]:
        """Deploy complete Redshift Serverless infrastructure"""
        if not self.redshift_role_arn:
            raise ValueError("Redshift role ARN must be set before deployment")
        
        self.logger.info("Starting Redshift Serverless deployment")
        results = {}
        
        try:
            # 1. Create namespace
            namespace_result = self.create_namespace()
            results['namespace'] = namespace_result
            
            # 2. Create workgroup
            workgroup_result = self.create_workgroup()
            results['workgroup'] = workgroup_result
            
            # 3. Create usage limits
            usage_limits_result = self.create_usage_limits()
            results['usage_limits'] = usage_limits_result
            
            # 4. Generate connection information
            endpoint = workgroup_result.get('endpoint', {})
            if endpoint:
                results['connection_info'] = {
                    'database_name': self.database_name,
                    'port': endpoint.get('port', 5439),
                    'endpoint_address': endpoint.get('address'),
                    'jdbc_url': f"jdbc:redshift://{endpoint.get('address')}:{endpoint.get('port', 5439)}/{self.database_name}" if endpoint.get('address') else None
                }
            
            # 5. Cost estimation
            results['cost_estimation'] = {
                'monthly_limit_rpu_hours': self.monthly_usage_limit,
                'estimated_min_monthly_cost_usd': self.monthly_usage_limit * 0.25,  # $0.25/RPU-hour minimum
                'estimated_max_monthly_cost_usd': self.monthly_usage_limit * 0.45   # $0.45/RPU-hour maximum
            }
            
            results['deployment_status'] = 'success'
            
            self.logger.info("Redshift Serverless deployment completed successfully")
            return results
            
        except Exception as e:
            self.logger.error(f"Redshift deployment failed: {e}")
            results['deployment_status'] = 'failed'
            results['error'] = str(e)
            return results
    
    def get_deployment_info(self) -> Dict[str, Any]:
        """Get deployment information"""
        return {
            'service': 'RedshiftServerless',
            'namespace_name': self.namespace_name,
            'workgroup_name': self.workgroup_name,
            'database_name': self.database_name,
            'master_username': self.master_username,
            'base_capacity_rpus': self.base_capacity_rpus,
            'max_capacity_rpus': self.max_capacity_rpus,
            'usage_limits_enabled': self.enable_usage_limits,
            'monthly_usage_limit': self.monthly_usage_limit if self.enable_usage_limits else None,
            'weekly_usage_limit': self.weekly_usage_limit if self.enable_usage_limits else None,
            'breach_action': self.usage_limit_breach_action,
            'publicly_accessible': self.publicly_accessible,
            'redshift_role_arn': self.redshift_role_arn,
            'region': self.aws_region,
            'environment': self.environment
        }
    
    def resource_exists(self, resource_identifier: str) -> bool:
        """Check if Redshift resource exists"""
        if resource_identifier == 'namespace':
            try:
                self.redshift_serverless_client.get_namespace(namespaceName=self.namespace_name)
                return True
            except ClientError:
                return False
        elif resource_identifier == 'workgroup':
            try:
                self.redshift_serverless_client.get_workgroup(workgroupName=self.workgroup_name)
                return True
            except ClientError:
                return False
        return False
    
    def delete_workgroup(self) -> Dict[str, str]:
        """Delete Redshift workgroup"""
        try:
            self.redshift_serverless_client.delete_workgroup(
                workgroupName=self.workgroup_name
            )
            self.logger.info(f"Deleted workgroup: {self.workgroup_name}")
            return {"status": "deleted"}
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                return {"status": "not_found"}
            else:
                self.handle_aws_error(e, f"deleting workgroup {self.workgroup_name}")
    
    def delete_namespace(self) -> Dict[str, str]:
        """Delete Redshift namespace"""
        try:
            self.redshift_serverless_client.delete_namespace(
                namespaceName=self.namespace_name
            )
            self.logger.info(f"Deleted namespace: {self.namespace_name}")
            return {"status": "deleted"}
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                return {"status": "not_found"}
            else:
                self.handle_aws_error(e, f"deleting namespace {self.namespace_name}")