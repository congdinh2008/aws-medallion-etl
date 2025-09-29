#!/usr/bin/env python3
"""
Usage Examples cho Meta Kaggle Pipeline Boto3 Deployment
Demonstrate different use cases và patterns
"""

import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

def example_basic_deployment():
    """Example 1: Basic deployment với default settings"""
    print("=== Example 1: Basic Deployment ===")
    
    from deploy import MetaKagglePipelineDeployer
    
    # Set required environment variable
    os.environ['REDSHIFT_MASTER_PASSWORD'] = 'MetaKaggle2024!'
    
    # Initialize deployer
    deployer = MetaKagglePipelineDeployer(environment='dev')
    
    # Validate configuration first
    try:
        deployer.validate_configuration()
        print("✅ Configuration is valid")
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        return
    
    # Show deployment plan
    plan = deployer.show_plan()
    print(f"📋 Planning to deploy {sum(len(v) if isinstance(v, dict) else 1 for v in plan['planned_resources'].values())} resource groups")
    
    # Deploy (commented out to prevent actual deployment in example)
    # results = deployer.deploy_full_pipeline()
    print("🚀 Deployment would start here...")

def example_foundation_only():
    """Example 2: Foundation-only deployment"""
    print("\n=== Example 2: Foundation Only ===")
    
    from deploy import MetaKagglePipelineDeployer
    
    # Custom configuration
    config_overrides = {
        'auto_deploy_advanced_services': False,
        'enable_cost_optimization': True
    }
    
    deployer = MetaKagglePipelineDeployer(environment='dev')
    # Override specific config values
    deployer.config.update(config_overrides)
    
    print("📦 Foundation deployment includes:")
    print("  - S3 Data Lake với Medallion architecture")
    print("  - IAM roles cho Glue và Redshift")
    print("  - IAM user cho Airflow")
    print("  - Auto-granting permissions")
    
    # Show what would be deployed
    s3_info = deployer.managers['s3'].get_deployment_info()
    iam_info = deployer.managers['iam'].get_deployment_info()
    
    print(f"📊 S3 bucket: {s3_info['bucket_name']}")
    print(f"🔐 IAM roles: {len(iam_info['roles'])} roles, {len(iam_info['users'])} users")

def example_custom_configuration():
    """Example 3: Custom configuration với specific requirements"""
    print("\n=== Example 3: Custom Configuration ===")
    
    from config.config_manager import ConfigurationManager
    
    # Custom config
    custom_config = {
        'aws_region': 'us-west-1',
        'environment': 'dev',
        's3_bucket_name': 'my-custom-meta-bucket',
        'redshift_database_name': 'custom_warehouse',
        'redshift_base_capacity_rpus': 16,  # Higher capacity
        'redshift_max_capacity_rpus': 128,
        'redshift_monthly_usage_limit': 1000,  # Higher limit
        'enable_cost_optimization': True,
        'common_tags': {
            'Project': 'CustomMetaPipeline',
            'Team': 'DataEngineering',
            'Environment': 'dev',
            'CostCenter': 'analytics'
        }
    }
    
    # Validate custom configuration
    config_manager = ConfigurationManager()
    try:
        config_manager.validate_config(custom_config)
        print("✅ Custom configuration is valid")
        
        # Show cost estimation
        monthly_rpu_limit = custom_config['redshift_monthly_usage_limit']
        estimated_min_cost = monthly_rpu_limit * 0.25
        estimated_max_cost = monthly_rpu_limit * 0.45
        print(f"💰 Estimated Redshift cost: ${estimated_min_cost:.2f} - ${estimated_max_cost:.2f}/month")
        
    except Exception as e:
        print(f"❌ Configuration validation failed: {e}")

def example_validation_workflow():
    """Example 4: Validation workflow"""
    print("\n=== Example 4: Validation Workflow ===")
    
    # Mock deployment results for demonstration
    mock_deployment_results = {
        's3': {
            'deployment_status': 'success',
            'bucket_name': 'psi-de-glue-congdinh',
            'folders_created': ['meta/raw/', 'meta/bronze/', 'meta/silver/', 'meta/gold/']
        },
        'iam': {
            'deployment_status': 'success',
            'glue_role': {'role_name': 'meta-dev-glue-execution-role'},
            'redshift_role': {'role_name': 'meta-dev-redshift-service-role'},
            'current_user_info': {'username': 'test-user'}
        }
    }
    
    # Load configuration
    from config.config_manager import ConfigurationManager
    config_manager = ConfigurationManager()
    config = config_manager.load_config('dev')
    
    # Note: Validation would require actual AWS resources
    print("🔍 Validation workflow includes:")
    print("  - S3 bucket existence và configuration")
    print("  - IAM roles và policy attachments")
    print("  - Glue databases và crawlers")
    print("  - Redshift Serverless resources")
    print("  - Connectivity testing")
    
    # Show mock validation results
    print("\n📊 Mock Validation Results:")
    for service, results in mock_deployment_results.items():
        status_icon = "✅" if results['deployment_status'] == 'success' else "❌"
        print(f"  {status_icon} {service.upper()} service validation")

def example_multi_environment():
    """Example 5: Multi-environment deployment patterns"""
    print("\n=== Example 5: Multi-Environment Deployment ===")
    
    environments = {
        'dev': {
            'redshift_base_capacity_rpus': 8,
            'redshift_monthly_usage_limit': 500,
            'enable_cost_optimization': True,
            'auto_deploy_advanced_services': True
        },
        'staging': {
            'redshift_base_capacity_rpus': 16,
            'redshift_monthly_usage_limit': 1000,
            'enable_cost_optimization': True,
            'auto_deploy_advanced_services': True
        },
        'prod': {
            'redshift_base_capacity_rpus': 32,
            'redshift_monthly_usage_limit': 2000,
            'enable_cost_optimization': False,  # Keep data available
            'auto_deploy_advanced_services': True
        }
    }
    
    print("🌍 Environment Configuration Patterns:")
    for env, config in environments.items():
        print(f"\n📍 {env.upper()} Environment:")
        print(f"  - Redshift Capacity: {config['redshift_base_capacity_rpus']} RPUs")
        print(f"  - Monthly Limit: {config['redshift_monthly_usage_limit']} RPU-hours")
        print(f"  - Cost Optimization: {'Enabled' if config['enable_cost_optimization'] else 'Disabled'}")
        
        # Cost estimation
        estimated_cost = config['redshift_monthly_usage_limit'] * 0.35  # Average
        print(f"  - Est. Monthly Cost: ~${estimated_cost:.2f}")

def example_error_handling():
    """Example 6: Error handling patterns"""
    print("\n=== Example 6: Error Handling Patterns ===")
    
    from deploy import MetaKagglePipelineDeployer
    
    # Common error scenarios và how to handle them
    error_scenarios = {
        'missing_password': {
            'error': 'Redshift password not provided',
            'solution': 'export REDSHIFT_MASTER_PASSWORD="SecurePassword123!"'
        },
        'invalid_bucket_name': {
            'error': 'S3 bucket name contains invalid characters',
            'solution': 'Update s3_bucket_name in config with valid name'
        },
        'insufficient_permissions': {
            'error': 'User not authorized to perform iam:CreateRole',
            'solution': 'Ensure AWS user has admin permissions or required policies'
        },
        'region_mismatch': {
            'error': 'Bucket exists in different region',
            'solution': 'Update aws_region in config hoặc change bucket name'
        }
    }
    
    print("🚨 Common Error Scenarios và Solutions:")
    for scenario, details in error_scenarios.items():
        print(f"\n❌ {scenario}:")
        print(f"   Error: {details['error']}")
        print(f"   Solution: {details['solution']}")
    
    # Show validation workflow for error prevention
    print(f"\n✅ Error Prevention với Validation:")
    print(f"   1. python deploy.py --validate")
    print(f"   2. python deploy.py --plan") 
    print(f"   3. python deploy.py --foundation-only")
    print(f"   4. python deploy.py")

def example_monitoring_and_maintenance():
    """Example 7: Monitoring và maintenance workflows"""
    print("\n=== Example 7: Monitoring & Maintenance ===")
    
    # Monitoring commands
    monitoring_commands = [
        {
            'command': 'python utils/validator.py --environment dev',
            'purpose': 'Comprehensive resource validation'
        },
        {
            'command': 'aws s3 ls s3://psi-de-glue-congdinh/meta/ --recursive',
            'purpose': 'Check S3 data structure'
        },
        {
            'command': 'aws glue get-databases',
            'purpose': 'List Glue databases'
        },
        {
            'command': 'aws redshift-serverless list-workgroups',
            'purpose': 'Check Redshift workgroups'
        }
    ]
    
    print("📊 Monitoring Commands:")
    for cmd_info in monitoring_commands:
        print(f"  • {cmd_info['command']}")
        print(f"    Purpose: {cmd_info['purpose']}")
    
    # Maintenance tasks
    print(f"\n🔧 Maintenance Tasks:")
    print(f"  • Weekly: Run validation checks")
    print(f"  • Monthly: Review cost reports")
    print(f"  • Quarterly: Update usage limits")
    print(f"  • As needed: Clean up old data")

def example_cost_optimization():
    """Example 8: Cost optimization strategies"""
    print("\n=== Example 8: Cost Optimization ===")
    
    cost_strategies = {
        'S3 Lifecycle': {
            'description': 'Automatic data tiering',
            'settings': {
                'raw_data': 'Standard → IA (30d) → Glacier (90d)',
                'bronze_data': 'Standard → IA (60d) → Glacier (180d)',
                'silver_data': 'Standard → IA (90d) → Glacier (365d)'
            }
        },
        'Redshift Usage Limits': {
            'description': 'RPU-hour caps để prevent overspend',
            'settings': {
                'dev': '500 RPU-hours/month (~$125-225)',
                'staging': '1000 RPU-hours/month (~$250-450)',
                'prod': '2000 RPU-hours/month (~$500-900)'
            }
        },
        'Intelligent Tiering': {
            'description': 'AWS S3 Intelligent-Tiering',
            'settings': {
                'archive_access': '1 day',
                'deep_archive_access': '90 days'
            }
        }
    }
    
    print("💰 Cost Optimization Strategies:")
    for strategy, details in cost_strategies.items():
        print(f"\n📊 {strategy}:")
        print(f"   {details['description']}")
        for setting, value in details['settings'].items():
            print(f"   • {setting}: {value}")

def main():
    """Run all examples"""
    print("🎯 Meta Kaggle Pipeline Boto3 Deployment Examples")
    print("=" * 60)
    
    try:
        # Run examples (most are safe demonstration code)
        example_basic_deployment()
        example_foundation_only()
        example_custom_configuration()
        example_validation_workflow()
        example_multi_environment()
        example_error_handling()
        example_monitoring_and_maintenance()
        example_cost_optimization()
        
        print("\n" + "=" * 60)
        print("🎉 All examples completed!")
        print("💡 To actually deploy: python deploy.py")
        print("🔍 To validate: python deploy.py --validate")
        print("📋 To see plan: python deploy.py --plan")
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("💡 Ensure you're running from the boto3_deployment directory")
        print("💡 Install requirements: pip install -r requirements.txt")
    except Exception as e:
        print(f"❌ Example error: {e}")

if __name__ == "__main__":
    main()