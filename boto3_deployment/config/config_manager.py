#!/usr/bin/env python3
"""
Configuration Management cho Meta Kaggle Pipeline Boto3 Deployment
Tương tự terraform.tfvars nhưng cho Python boto3
"""

import json
import yaml
import os
from typing import Dict, Any, Optional
from dataclasses import dataclass
import logging


@dataclass
class DeploymentConfig:
    """Configuration data class tương tự terraform variables"""
    
    # General Configuration
    aws_region: str = "us-west-1"
    environment: str = "dev"
    project_name: str = "meta-kaggle-pipeline"
    
    # S3 Configuration
    s3_bucket_name: str = "psi-de-glue-congdinh"
    
    # Redshift Configuration
    redshift_database_name: str = "meta_warehouse"
    redshift_master_username: str = "meta_admin"
    redshift_master_password: Optional[str] = None
    redshift_base_capacity_rpus: int = 8
    redshift_max_capacity_rpus: int = 64
    redshift_publicly_accessible: bool = False
    
    # Cost Control
    enable_redshift_usage_limits: bool = True
    redshift_monthly_usage_limit: int = 500
    redshift_weekly_usage_limit: int = 125
    redshift_usage_limit_breach_action: str = "log"
    
    # Feature Flags
    auto_deploy_advanced_services: bool = True
    enable_secrets_manager: bool = False
    enable_cost_optimization: bool = True
    
    # Network Configuration
    private_subnet_ids: list = None
    redshift_security_group_ids: list = None
    
    # Tags
    common_tags: Dict[str, str] = None
    
    def __post_init__(self):
        """Thiết lập default values sau khi init"""
        if self.private_subnet_ids is None:
            self.private_subnet_ids = []
            
        if self.redshift_security_group_ids is None:
            self.redshift_security_group_ids = []
            
        if self.common_tags is None:
            self.common_tags = {
                "Project": "meta-kaggle-pipeline",
                "Environment": self.environment,
                "Owner": "data-team",
                "ManagedBy": "boto3-deployment",
                "Purpose": "etl-pipeline",
                "Region": self.aws_region,
                "CostCenter": "data-engineering"
            }


class ConfigurationManager:
    """Quản lý configuration cho boto3 deployment"""
    
    def __init__(self, config_dir: str = None):
        """
        Khởi tạo configuration manager
        
        Args:
            config_dir: Thư mục chứa config files
        """
        self.config_dir = config_dir or os.path.join(
            os.path.dirname(__file__), '..', 'config'
        )
        self.logger = logging.getLogger('meta_pipeline.config')
    
    def load_config(self, environment: str = "dev") -> Dict[str, Any]:
        """
        Load configuration cho environment
        
        Args:
            environment: Environment name (dev, staging, prod)
            
        Returns:
            Configuration dictionary
        """
        config_file = os.path.join(self.config_dir, f"{environment}.yaml")
        
        if not os.path.exists(config_file):
            self.logger.warning(f"Config file not found: {config_file}")
            return self._get_default_config()
        
        try:
            with open(config_file, 'r') as f:
                config_data = yaml.safe_load(f)
            
            # Override với environment variables
            config_data = self._override_with_env_vars(config_data)
            
            self.logger.info(f"Loaded configuration from {config_file}")
            return config_data
            
        except Exception as e:
            self.logger.error(f"Failed to load config from {config_file}: {e}")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Lấy default configuration"""
        default_config = DeploymentConfig()
        return {
            'aws_region': default_config.aws_region,
            'environment': default_config.environment,
            'project_name': default_config.project_name,
            's3_bucket_name': default_config.s3_bucket_name,
            'redshift_database_name': default_config.redshift_database_name,
            'redshift_master_username': default_config.redshift_master_username,
            'redshift_master_password': default_config.redshift_master_password,
            'redshift_base_capacity_rpus': default_config.redshift_base_capacity_rpus,
            'redshift_max_capacity_rpus': default_config.redshift_max_capacity_rpus,
            'redshift_publicly_accessible': default_config.redshift_publicly_accessible,
            'enable_redshift_usage_limits': default_config.enable_redshift_usage_limits,
            'redshift_monthly_usage_limit': default_config.redshift_monthly_usage_limit,
            'redshift_weekly_usage_limit': default_config.redshift_weekly_usage_limit,
            'redshift_usage_limit_breach_action': default_config.redshift_usage_limit_breach_action,
            'auto_deploy_advanced_services': default_config.auto_deploy_advanced_services,
            'enable_secrets_manager': default_config.enable_secrets_manager,
            'enable_cost_optimization': default_config.enable_cost_optimization,
            'private_subnet_ids': default_config.private_subnet_ids,
            'redshift_security_group_ids': default_config.redshift_security_group_ids,
            'common_tags': default_config.common_tags
        }
    
    def _override_with_env_vars(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Override config với environment variables"""
        env_mappings = {
            'AWS_REGION': 'aws_region',
            'ENVIRONMENT': 'environment',
            'S3_BUCKET_NAME': 's3_bucket_name',
            'REDSHIFT_MASTER_PASSWORD': 'redshift_master_password',
            'REDSHIFT_DATABASE_NAME': 'redshift_database_name',
            'REDSHIFT_MASTER_USERNAME': 'redshift_master_username',
            'AUTO_DEPLOY_ADVANCED_SERVICES': 'auto_deploy_advanced_services',
            'ENABLE_SECRETS_MANAGER': 'enable_secrets_manager',
            'ENABLE_COST_OPTIMIZATION': 'enable_cost_optimization'
        }
        
        for env_var, config_key in env_mappings.items():
            env_value = os.getenv(env_var)
            if env_value is not None:
                # Handle boolean values
                if config_key in ['auto_deploy_advanced_services', 'enable_secrets_manager', 
                                'enable_cost_optimization', 'redshift_publicly_accessible',
                                'enable_redshift_usage_limits']:
                    config[config_key] = env_value.lower() in ('true', '1', 'yes', 'on')
                # Handle integer values  
                elif config_key in ['redshift_base_capacity_rpus', 'redshift_max_capacity_rpus',
                                  'redshift_monthly_usage_limit', 'redshift_weekly_usage_limit']:
                    config[config_key] = int(env_value)
                # Handle string values
                else:
                    config[config_key] = env_value
                    
                self.logger.info(f"Config override from env var {env_var}: {config_key}")
        
        return config
    
    def validate_config(self, config: Dict[str, Any]) -> bool:
        """
        Validate configuration
        
        Args:
            config: Configuration dictionary
            
        Returns:
            True nếu valid, raise exception nếu invalid
        """
        required_fields = [
            'aws_region', 'environment', 's3_bucket_name'
        ]
        
        # Check required fields
        for field in required_fields:
            if field not in config or not config[field]:
                raise ValueError(f"Required configuration field missing: {field}")
        
        # Validate Redshift password requirements nếu có
        if config.get('redshift_master_password'):
            password = config['redshift_master_password']
            if len(password) < 8 or len(password) > 64:
                raise ValueError("Redshift password must be 8-64 characters")
            
            # Check complexity requirements
            has_upper = any(c.isupper() for c in password)
            has_lower = any(c.islower() for c in password)  
            has_digit = any(c.isdigit() for c in password)
            
            if not (has_upper and has_lower and has_digit):
                raise ValueError(
                    "Redshift password must contain uppercase, lowercase, and digits"
                )
        
        # Validate region
        valid_regions = [
            'us-east-1', 'us-east-2', 'us-west-1', 'us-west-2',
            'eu-west-1', 'eu-central-1', 'ap-southeast-1', 'ap-northeast-1'
        ]
        if config['aws_region'] not in valid_regions:
            self.logger.warning(f"Unusual AWS region: {config['aws_region']}")
        
        # Validate Redshift capacity
        base_capacity = config.get('redshift_base_capacity_rpus', 8)
        max_capacity = config.get('redshift_max_capacity_rpus', 64)
        
        if base_capacity < 8:
            raise ValueError("Redshift base capacity must be at least 8 RPUs")
        if max_capacity < base_capacity:
            raise ValueError("Redshift max capacity must be >= base capacity")
        
        self.logger.info("Configuration validation passed")
        return True
    
    def save_config(self, config: Dict[str, Any], environment: str = "dev") -> None:
        """
        Save configuration to file
        
        Args:
            config: Configuration dictionary
            environment: Environment name
        """
        config_file = os.path.join(self.config_dir, f"{environment}.yaml")
        
        try:
            os.makedirs(self.config_dir, exist_ok=True)
            
            # Remove sensitive fields khi save
            safe_config = config.copy()
            if 'redshift_master_password' in safe_config:
                safe_config['redshift_master_password'] = "***MASKED***"
            
            with open(config_file, 'w') as f:
                yaml.dump(safe_config, f, default_flow_style=False, indent=2)
            
            self.logger.info(f"Configuration saved to {config_file}")
            
        except Exception as e:
            self.logger.error(f"Failed to save config to {config_file}: {e}")
            raise
    
    def get_terraform_equivalent(self, config: Dict[str, Any]) -> str:
        """
        Generate Terraform equivalent configuration for comparison
        
        Args:
            config: Configuration dictionary
            
        Returns:
            Terraform variables format string
        """
        terraform_vars = []
        
        # String variables
        for key in ['aws_region', 'environment', 'project_name', 's3_bucket_name',
                   'redshift_database_name', 'redshift_master_username',
                   'redshift_usage_limit_breach_action']:
            if key in config:
                terraform_vars.append(f'{key} = "{config[key]}"')
        
        # Integer variables  
        for key in ['redshift_base_capacity_rpus', 'redshift_max_capacity_rpus',
                   'redshift_monthly_usage_limit', 'redshift_weekly_usage_limit']:
            if key in config:
                terraform_vars.append(f'{key} = {config[key]}')
        
        # Boolean variables
        for key in ['redshift_publicly_accessible', 'enable_redshift_usage_limits',
                   'auto_deploy_advanced_services', 'enable_secrets_manager',
                   'enable_cost_optimization']:
            if key in config:
                terraform_vars.append(f'{key} = {str(config[key]).lower()}')
        
        # Tags
        if 'common_tags' in config:
            tags_str = "common_tags = {\n"
            for k, v in config['common_tags'].items():
                tags_str += f'  {k} = "{v}"\n'
            tags_str += "}"
            terraform_vars.append(tags_str)
        
        return '\n'.join(terraform_vars)