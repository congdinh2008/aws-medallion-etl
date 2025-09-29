#!/usr/bin/env python3
"""
Main Deployment Orchestrator cho Meta Kaggle Pipeline
Tương tự terraform deploy.sh nhưng sử dụng boto3
"""

import os
import sys
import logging
import json
import secrets
import string
import time
from typing import Dict, List, Any, Optional
from pathlib import Path

# Add current directory to path for imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from config.config_manager import ConfigurationManager
from managers.s3_manager import S3Manager
from managers.iam_manager import IAMManager
from managers.glue_manager import GlueManager
from managers.redshift_manager import RedshiftManager


class MetaKagglePipelineDeployer:
    """
    Main orchestrator cho deployment of Meta Kaggle Pipeline
    Tương tự functionality của terraform deploy.sh
    """
    
    def __init__(self, environment: str = "dev", config_dir: Optional[str] = None):
        """
        Khởi tạo deployer
        
        Args:
            environment: Target environment (dev, staging, prod)
            config_dir: Custom config directory path
        """
        self.environment = environment
        self.config_manager = ConfigurationManager(config_dir)
        
        # Load configuration
        self.config = self.config_manager.load_config(environment)
        
        # Setup logging
        self.logger = self._setup_logger()
        
        # Initialize managers
        self.managers = {}
        self._initialize_managers()
        
        # Deployment results storage
        self.deployment_results = {}
        
    def _setup_logger(self) -> logging.Logger:
        """Setup comprehensive logging"""
        logger = logging.getLogger('meta_pipeline_deployer')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            # Console handler
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            
            # File handler
            log_dir = Path(__file__).parent / "logs"
            log_dir.mkdir(exist_ok=True)
            
            file_handler = logging.FileHandler(
                log_dir / f"deployment_{self.environment}.log"
            )
            file_handler.setLevel(logging.DEBUG)
            
            # Formatter
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(formatter)
            file_handler.setFormatter(formatter)
            
            logger.addHandler(console_handler)
            logger.addHandler(file_handler)
        
        return logger
    
    def _initialize_managers(self) -> None:
        """Initialize all infrastructure managers"""
        try:
            # S3 Manager - Foundation
            self.managers['s3'] = S3Manager(self.config)
            
            # IAM Manager - Foundation  
            self.managers['iam'] = IAMManager(self.config)
            
            # Advanced services (conditional based on config)
            if self.config.get('auto_deploy_advanced_services', True):
                self.managers['glue'] = GlueManager(self.config)
                self.managers['redshift'] = RedshiftManager(self.config)
            
            self.logger.info(f"Initialized {len(self.managers)} managers")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize managers: {e}")
            raise
    
    def print_header(self) -> None:
        """Print deployment header"""
        header = f"""
╔══════════════════════════════════════════════════════════════════════════════════╗
║                    🚀 Meta Kaggle Pipeline Deployment (Boto3)                   ║
║                        Infrastructure as Code with Python                        ║
║                                                                                  ║
║  Environment: {self.environment:<10}                                                        ║
║  Region:      {self.config.get('aws_region', 'us-west-1'):<10}                                                        ║
║  Bucket:      {self.config.get('s3_bucket_name', 'N/A'):<30}                                    ║
╚══════════════════════════════════════════════════════════════════════════════════╝
"""
        print(header)
    
    def generate_secure_redshift_password(self) -> str:
        """
        Generate secure Redshift password
        Tương tự logic trong terraform deploy.sh
        """
        # Character sets (avoid confusing characters)
        upper = "ABCDEFGHJKLMNPQRSTUVWXYZ"
        lower = "abcdefghijkmnpqrstuvwxyz" 
        numbers = "23456789"
        special = "!#%&*+,./:;=?^_{}~-"
        
        # Ensure at least one of each type
        password_chars = [
            secrets.choice(upper),
            secrets.choice(lower),
            secrets.choice(numbers),
            secrets.choice(special)
        ]
        
        # Fill remaining length
        all_chars = upper + lower + numbers + special
        for _ in range(8):  # Total length 12 characters
            password_chars.append(secrets.choice(all_chars))
        
        # Shuffle the list
        secrets.SystemRandom().shuffle(password_chars)
        
        return ''.join(password_chars)
    
    def validate_configuration(self) -> bool:
        """
        Validate full configuration before deployment
        Tương tự validate_terraform() function
        """
        self.logger.info("🔍 Validating configuration...")
        
        try:
            # Basic config validation
            self.config_manager.validate_config(self.config)
            
            # Generate Redshift password if not provided
            if not self.config.get('redshift_master_password'):
                self.config['redshift_master_password'] = self.generate_secure_redshift_password()
                self.logger.info("✓ Generated secure Redshift password")
            
            # Validate each manager's configuration
            for name, manager in self.managers.items():
                manager.validate_config()
                self.logger.info(f"✓ {name.upper()} configuration valid")
            
            self.logger.info("✅ Configuration validation passed")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Configuration validation failed: {e}")
            raise
    
    def deploy_foundation(self) -> Dict[str, Any]:
        """
        Deploy foundation infrastructure (S3 + IAM)
        Tương tự --foundation-only option
        """
        self.logger.info("🏗️ Starting foundation deployment...")
        foundation_results = {}
        
        try:
            # Deploy S3 Data Lake
            self.logger.info("📦 Deploying S3 Data Lake...")
            s3_result = self.managers['s3'].deploy()
            foundation_results['s3'] = s3_result
            
            if s3_result['deployment_status'] != 'success':
                raise Exception(f"S3 deployment failed: {s3_result.get('error')}")
            
            # Deploy IAM resources
            self.logger.info("🔐 Deploying IAM resources...")
            iam_result = self.managers['iam'].deploy()
            foundation_results['iam'] = iam_result
            
            if iam_result['deployment_status'] != 'success':
                raise Exception(f"IAM deployment failed: {iam_result.get('error')}")
            
            # Store role ARNs for advanced services
            self.glue_role_arn = iam_result['glue_role']['role_arn']
            self.redshift_role_arn = iam_result['redshift_role']['role_arn']
            
            # Auto-grant destroy permissions to current user
            self.logger.info("🔑 Auto-granting destroy permissions to current user...")
            destroy_policy_result = self.managers['iam'].create_and_attach_destroy_policy()
            
            if destroy_policy_result.get('success'):
                foundation_results['destroy_policy'] = destroy_policy_result
                self.logger.info(f"✅ Destroy permissions granted to: {destroy_policy_result.get('attached_to')}")
            else:
                self.logger.warning(f"⚠️ Failed to auto-grant destroy permissions: {destroy_policy_result.get('error')}")
                # Don't fail deployment for this, just log warning
            
            foundation_results['deployment_status'] = 'success'
            self.logger.info("✅ Foundation deployment completed successfully")
            
            return foundation_results
            
        except Exception as e:
            self.logger.error(f"❌ Foundation deployment failed: {e}")
            foundation_results['deployment_status'] = 'failed'
            foundation_results['error'] = str(e)
            raise
    
    def deploy_advanced_services(self) -> Dict[str, Any]:
        """
        Deploy advanced services (Glue + Redshift)
        Tương tự advanced services trong terraform
        """
        self.logger.info("🚀 Starting advanced services deployment...")
        advanced_results = {}
        
        try:
            # Deploy Glue resources
            if 'glue' in self.managers:
                self.logger.info("🕷️ Deploying Glue resources...")
                # Set role ARN from foundation deployment
                self.managers['glue'].set_glue_role_arn(self.glue_role_arn)
                
                glue_result = self.managers['glue'].deploy()
                advanced_results['glue'] = glue_result
                
                if glue_result['deployment_status'] != 'success':
                    raise Exception(f"Glue deployment failed: {glue_result.get('error')}")
            
            # Deploy Redshift Serverless
            if 'redshift' in self.managers:
                self.logger.info("🏢 Deploying Redshift Serverless...")
                # Set role ARN from foundation deployment
                self.managers['redshift'].set_redshift_role_arn(self.redshift_role_arn)
                
                redshift_result = self.managers['redshift'].deploy()
                advanced_results['redshift'] = redshift_result
                
                if redshift_result['deployment_status'] != 'success':
                    raise Exception(f"Redshift deployment failed: {redshift_result.get('error')}")
            
            advanced_results['deployment_status'] = 'success'
            self.logger.info("✅ Advanced services deployment completed successfully")
            
            return advanced_results
            
        except Exception as e:
            self.logger.error(f"❌ Advanced services deployment failed: {e}")
            advanced_results['deployment_status'] = 'failed'
            advanced_results['error'] = str(e)
            raise
    
    def deploy_full_pipeline(self) -> Dict[str, Any]:
        """
        Deploy complete pipeline (Foundation + Advanced Services)
        Main deployment method
        """
        self.logger.info("🎯 Starting full pipeline deployment...")
        
        try:
            # Print deployment header
            self.print_header()
            
            # Validate configuration
            self.validate_configuration()
            
            # Deploy foundation
            foundation_results = self.deploy_foundation()
            self.deployment_results.update(foundation_results)
            
            # Deploy advanced services if enabled
            if self.config.get('auto_deploy_advanced_services', True):
                advanced_results = self.deploy_advanced_services()
                self.deployment_results.update(advanced_results)
            else:
                self.logger.info("ℹ️ Advanced services deployment disabled")
            
            # Final status
            self.deployment_results['overall_status'] = 'success'
            self.deployment_results['environment'] = self.environment
            self.deployment_results['region'] = self.config.get('aws_region')
            
            # Generate deployment summary
            self._print_deployment_summary()
            
            return self.deployment_results
            
        except Exception as e:
            self.logger.error(f"❌ Full pipeline deployment failed: {e}")
            self.deployment_results['overall_status'] = 'failed'
            self.deployment_results['error'] = str(e)
            return self.deployment_results
    
    def _print_deployment_summary(self) -> None:
        """Print comprehensive deployment summary"""
        print(f"\n{'='*80}")
        print("📊 DEPLOYMENT SUMMARY")
        print(f"{'='*80}")
        
        print(f"Environment: {self.environment}")
        print(f"Region: {self.config.get('aws_region')}")
        print(f"Status: {'✅ SUCCESS' if self.deployment_results.get('overall_status') == 'success' else '❌ FAILED'}")
        
        print(f"\n📦 S3 DATA LAKE:")
        if 's3' in self.deployment_results:
            s3_result = self.deployment_results['s3']
            print(f"  Bucket: {s3_result.get('bucket_name', 'N/A')}")
            print(f"  Folders: {len(s3_result.get('folders_created', []))}")
            print(f"  Status: {'✅' if s3_result.get('deployment_status') == 'success' else '❌'}")
        
        print(f"\n🔐 IAM RESOURCES:")
        if 'iam' in self.deployment_results:
            iam_result = self.deployment_results['iam']
            print(f"  Current User: {iam_result.get('current_user_info', {}).get('username', 'N/A')}")
            print(f"  Glue Role: {iam_result.get('glue_role', {}).get('role_name', 'N/A')}")
            print(f"  Redshift Role: {iam_result.get('redshift_role', {}).get('role_name', 'N/A')}")
            print(f"  Airflow User: {iam_result.get('airflow_user', {}).get('user_name', 'N/A')}")
            print(f"  Status: {'✅' if iam_result.get('deployment_status') == 'success' else '❌'}")
        
        if 'glue' in self.deployment_results:
            print(f"\n🕷️ GLUE RESOURCES:")
            glue_result = self.deployment_results['glue']
            if 'databases' in glue_result:
                print(f"  Databases: {len(glue_result['databases'].get('all_databases', []))}")
            if 'bronze_crawlers' in glue_result:
                print(f"  Bronze Crawlers: {len(glue_result['bronze_crawlers'].get('created_crawlers', []))}")
            if 'athena' in glue_result:
                print(f"  Athena Workgroup: {glue_result['athena'].get('workgroup_name', 'N/A')}")
            print(f"  Status: {'✅' if glue_result.get('deployment_status') == 'success' else '❌'}")
        
        if 'redshift' in self.deployment_results:
            print(f"\n🏢 REDSHIFT SERVERLESS:")
            redshift_result = self.deployment_results['redshift']
            if 'namespace' in redshift_result:
                print(f"  Namespace: {redshift_result['namespace'].get('namespace_name', 'N/A')}")
            if 'workgroup' in redshift_result:
                print(f"  Workgroup: {redshift_result['workgroup'].get('workgroup_name', 'N/A')}")
            if 'cost_estimation' in redshift_result:
                cost = redshift_result['cost_estimation']
                print(f"  Est. Monthly Cost: ${cost.get('estimated_min_monthly_cost_usd', 0):.2f} - ${cost.get('estimated_max_monthly_cost_usd', 0):.2f}")
            print(f"  Status: {'✅' if redshift_result.get('deployment_status') == 'success' else '❌'}")
        
        print(f"\n🔗 USEFUL CONNECTIONS:")
        if 'redshift' in self.deployment_results and 'connection_info' in self.deployment_results['redshift']:
            conn = self.deployment_results['redshift']['connection_info']
            if conn.get('jdbc_url'):
                print(f"  Redshift JDBC: {conn['jdbc_url']}")
        
        if 'glue' in self.deployment_results and 'athena' in self.deployment_results['glue']:
            athena = self.deployment_results['glue']['athena']
            print(f"  Athena Workgroup: {athena.get('workgroup_name', 'N/A')}")
        
        print(f"\n{'='*80}")
    
    def validate_only(self) -> bool:
        """
        Validate configuration without deployment
        Tương tự --validate option
        """
        try:
            self.print_header()
            self.logger.info("🔍 Validation mode - no resources will be created")
            
            valid = self.validate_configuration()
            
            if valid:
                print("✅ Configuration validation passed - ready for deployment!")
                return True
            else:
                print("❌ Configuration validation failed")
                return False
                
        except Exception as e:
            print(f"❌ Validation failed: {e}")
            return False
    
    def show_plan(self) -> Dict[str, Any]:
        """
        Show deployment plan without executing
        Tương tự --plan option
        """
        self.print_header()
        self.logger.info("📋 Plan mode - showing planned resources")
        
        plan = {
            'environment': self.environment,
            'region': self.config.get('aws_region'),
            'planned_resources': {}
        }
        
        # S3 resources
        s3_info = self.managers['s3'].get_deployment_info()
        plan['planned_resources']['s3'] = {
            'bucket': s3_info['bucket_name'],
            'folders': len(s3_info['medallion_layers']) * len(s3_info['tables']) + 10,  # Approximate
            'data_paths': s3_info['data_paths']
        }
        
        # IAM resources
        iam_info = self.managers['iam'].get_deployment_info()
        plan['planned_resources']['iam'] = {
            'roles': len(iam_info['roles']),
            'users': len(iam_info['users']),
            'policies': len(iam_info['policies'])
        }
        
        # Advanced services
        if self.config.get('auto_deploy_advanced_services', True):
            if 'glue' in self.managers:
                glue_info = self.managers['glue'].get_deployment_info()
                plan['planned_resources']['glue'] = {
                    'databases': len(glue_info['databases']),
                    'crawlers': sum(glue_info['crawler_counts'].values()),
                    'jobs': sum(glue_info['job_counts'].values()),
                    'athena_workgroup': glue_info['athena_workgroup']
                }
            
            if 'redshift' in self.managers:
                redshift_info = self.managers['redshift'].get_deployment_info()
                plan['planned_resources']['redshift'] = {
                    'namespace': redshift_info['namespace_name'],
                    'workgroup': redshift_info['workgroup_name'],
                    'database': redshift_info['database_name'],
                    'capacity_rpus': f"{redshift_info['base_capacity_rpus']}-{redshift_info['max_capacity_rpus']}",
                    'usage_limits': redshift_info['usage_limits_enabled']
                }
        
        # Pretty print plan
        self._print_plan(plan)
        return plan
    
    def _print_plan(self, plan: Dict[str, Any]) -> None:
        """Print deployment plan in readable format"""
        print(f"\n📋 DEPLOYMENT PLAN")
        print(f"{'='*50}")
        print(f"Environment: {plan['environment']}")
        print(f"Region: {plan['region']}")
        
        resources = plan['planned_resources']
        
        if 's3' in resources:
            print(f"\n📦 S3 Resources:")
            print(f"  └── Bucket: {resources['s3']['bucket']}")
            print(f"  └── Folders: ~{resources['s3']['folders']}")
        
        if 'iam' in resources:
            print(f"\n🔐 IAM Resources:")
            print(f"  ├── Roles: {resources['iam']['roles']}")
            print(f"  ├── Users: {resources['iam']['users']}")
            print(f"  └── Policies: {resources['iam']['policies']}")
        
        if 'glue' in resources:
            print(f"\n🕷️ Glue Resources:")
            print(f"  ├── Databases: {resources['glue']['databases']}")
            print(f"  ├── Crawlers: {resources['glue']['crawlers']}")
            print(f"  ├── Jobs: {resources['glue']['jobs']}")
            print(f"  └── Athena Workgroup: {resources['glue']['athena_workgroup']}")
        
        if 'redshift' in resources:
            print(f"\n🏢 Redshift Resources:")
            print(f"  ├── Namespace: {resources['redshift']['namespace']}")
            print(f"  ├── Workgroup: {resources['redshift']['workgroup']}")
            print(f"  ├── Database: {resources['redshift']['database']}")
            print(f"  ├── Capacity: {resources['redshift']['capacity_rpus']} RPUs")
            print(f"  └── Usage Limits: {'Enabled' if resources['redshift']['usage_limits'] else 'Disabled'}")
        
        print(f"\n{'='*50}")
    
    def save_deployment_state(self, filename: Optional[str] = None) -> str:
        """Save deployment state to JSON file"""
        if not filename:
            timestamp = int(time.time())
            filename = f"deployment_state_{self.environment}_{timestamp}.json"
        
        state_file = Path(__file__).parent / "logs" / filename
        state_file.parent.mkdir(exist_ok=True)
        
        with open(state_file, 'w') as f:
            json.dump(self.deployment_results, f, indent=2, default=str)
        
        self.logger.info(f"Deployment state saved to: {state_file}")
        return str(state_file)


def main():
    """Main CLI entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Meta Kaggle Pipeline Deployment with Boto3",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python deploy.py                          # Full deployment (dev)
  python deploy.py --foundation-only        # Deploy S3 + IAM only
  python deploy.py --validate              # Validate configuration
  python deploy.py --plan                  # Show deployment plan
  python deploy.py --environment prod      # Deploy to production
        """
    )
    
    parser.add_argument(
        '--environment', '-e',
        default='dev',
        choices=['dev', 'staging', 'prod'],
        help='Target environment (default: dev)'
    )
    
    parser.add_argument(
        '--foundation-only',
        action='store_true',
        help='Deploy only S3 + IAM foundation'
    )
    
    parser.add_argument(
        '--validate',
        action='store_true',
        help='Validate configuration without deployment'
    )
    
    parser.add_argument(
        '--plan',
        action='store_true',
        help='Show deployment plan without executing'
    )
    
    parser.add_argument(
        '--config-dir',
        help='Custom configuration directory path'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Initialize deployer
        deployer = MetaKagglePipelineDeployer(
            environment=args.environment,
            config_dir=args.config_dir
        )
        
        # Execute based on arguments
        if args.validate:
            success = deployer.validate_only()
            sys.exit(0 if success else 1)
        elif args.plan:
            deployer.show_plan()
            sys.exit(0)
        elif args.foundation_only:
            deployer.config['auto_deploy_advanced_services'] = False
            results = deployer.deploy_full_pipeline()
        else:
            results = deployer.deploy_full_pipeline()
        
        # Save deployment state
        deployer.save_deployment_state()
        
        # Exit with appropriate code
        if results.get('overall_status') == 'success':
            print("\n🎉 Deployment completed successfully!")
            sys.exit(0)
        else:
            print("\n💥 Deployment failed!")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n⏹️ Deployment interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Deployment failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()