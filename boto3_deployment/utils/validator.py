#!/usr/bin/env python3
"""
Validation và Testing Framework cho Meta Kaggle Pipeline Boto3 Deployment
Kiểm tra resources, connectivity, và deployment integrity
"""

import logging
import time
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    # Fallback for environments without boto3
    pass

from managers.base_manager import BaseInfrastructureManager


class DeploymentValidator:
    """
    Validator cho deployment resources và connectivity
    """
    
    def __init__(self, config: Dict[str, Any], deployment_results: Dict[str, Any]):
        """
        Khởi tạo validator
        
        Args:
            config: Configuration dictionary
            deployment_results: Results từ deployment
        """
        self.config = config
        self.deployment_results = deployment_results
        self.region = config.get('aws_region', 'us-west-1')
        
        self.logger = logging.getLogger('deployment_validator')
        
        # Initialize AWS clients
        try:
            self.session = boto3.Session(region_name=self.region)
            self.s3_client = self.session.client('s3')
            self.iam_client = self.session.client('iam')
            self.glue_client = self.session.client('glue')
            self.redshift_serverless_client = self.session.client('redshift-serverless')
            self.athena_client = self.session.client('athena')
            self.sts_client = self.session.client('sts')
        except Exception as e:
            self.logger.error(f"Failed to initialize AWS clients: {e}")
            raise
    
    def validate_s3_resources(self) -> Dict[str, Any]:
        """Validate S3 bucket và folder structure"""
        self.logger.info("🔍 Validating S3 resources...")
        validation_results = {
            'service': 'S3',
            'checks': [],
            'overall_status': 'success'
        }
        
        bucket_name = self.config['s3_bucket_name']
        
        # Check 1: Bucket existence
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
            validation_results['checks'].append({
                'check': 'bucket_exists',
                'status': 'pass',
                'message': f"Bucket {bucket_name} exists"
            })
        except ClientError as e:
            validation_results['checks'].append({
                'check': 'bucket_exists',
                'status': 'fail',
                'message': f"Bucket {bucket_name} not found: {e}"
            })
            validation_results['overall_status'] = 'failed'
        
        # Check 2: Versioning enabled
        try:
            response = self.s3_client.get_bucket_versioning(Bucket=bucket_name)
            if response.get('Status') == 'Enabled':
                validation_results['checks'].append({
                    'check': 'versioning_enabled',
                    'status': 'pass',
                    'message': "Bucket versioning is enabled"
                })
            else:
                validation_results['checks'].append({
                    'check': 'versioning_enabled',
                    'status': 'fail',
                    'message': "Bucket versioning is not enabled"
                })
        except ClientError as e:
            validation_results['checks'].append({
                'check': 'versioning_enabled',
                'status': 'error',
                'message': f"Could not check versioning: {e}"
            })
        
        # Check 3: Encryption enabled
        try:
            response = self.s3_client.get_bucket_encryption(Bucket=bucket_name)
            if response.get('ServerSideEncryptionConfiguration'):
                validation_results['checks'].append({
                    'check': 'encryption_enabled',
                    'status': 'pass',
                    'message': "Bucket encryption is enabled"
                })
            else:
                validation_results['checks'].append({
                    'check': 'encryption_enabled',
                    'status': 'fail',
                    'message': "Bucket encryption is not enabled"
                })
        except ClientError as e:
            validation_results['checks'].append({
                'check': 'encryption_enabled',
                'status': 'error',
                'message': f"Could not check encryption: {e}"
            })
        
        # Check 4: Folder structure
        expected_folders = [
            'meta/raw/',
            'meta/bronze/',
            'meta/silver/',
            'meta/gold/',
            'meta/athena-results/',
            'scripts/glue_jobs/'
        ]
        
        folders_found = 0
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=bucket_name,
                Delimiter='/',
                MaxKeys=1000
            )
            
            existing_prefixes = []
            if 'CommonPrefixes' in response:
                existing_prefixes = [cp['Prefix'] for cp in response['CommonPrefixes']]
            
            for folder in expected_folders:
                if folder in existing_prefixes or self._check_folder_exists(bucket_name, folder):
                    folders_found += 1
            
            validation_results['checks'].append({
                'check': 'folder_structure',
                'status': 'pass' if folders_found >= len(expected_folders) * 0.8 else 'warning',
                'message': f"Found {folders_found}/{len(expected_folders)} expected folders"
            })
            
        except ClientError as e:
            validation_results['checks'].append({
                'check': 'folder_structure',
                'status': 'error',
                'message': f"Could not check folder structure: {e}"
            })
        
        return validation_results
    
    def _check_folder_exists(self, bucket_name: str, folder: str) -> bool:
        """Check if a specific folder exists in bucket"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=folder,
                MaxKeys=1
            )
            return 'Contents' in response
        except:
            return False
    
    def validate_iam_resources(self) -> Dict[str, Any]:
        """Validate IAM roles, users, và policies"""
        self.logger.info("🔍 Validating IAM resources...")
        validation_results = {
            'service': 'IAM',
            'checks': [],
            'overall_status': 'success'
        }
        
        if 'iam' not in self.deployment_results:
            validation_results['overall_status'] = 'skipped'
            return validation_results
        
        iam_results = self.deployment_results['iam']
        
        # Check 1: Glue execution role
        if 'glue_role' in iam_results:
            role_name = iam_results['glue_role']['role_name']
            try:
                self.iam_client.get_role(RoleName=role_name)
                validation_results['checks'].append({
                    'check': 'glue_role_exists',
                    'status': 'pass',
                    'message': f"Glue role {role_name} exists"
                })
                
                # Check attached policies
                response = self.iam_client.list_attached_role_policies(RoleName=role_name)
                attached_policies = len(response['AttachedPolicies'])
                validation_results['checks'].append({
                    'check': 'glue_role_policies',
                    'status': 'pass' if attached_policies > 0 else 'warning',
                    'message': f"Glue role has {attached_policies} attached policies"
                })
                
            except ClientError as e:
                validation_results['checks'].append({
                    'check': 'glue_role_exists',
                    'status': 'fail',
                    'message': f"Glue role not found: {e}"
                })
                validation_results['overall_status'] = 'failed'
        
        # Check 2: Redshift service role
        if 'redshift_role' in iam_results:
            role_name = iam_results['redshift_role']['role_name']
            try:
                self.iam_client.get_role(RoleName=role_name)
                validation_results['checks'].append({
                    'check': 'redshift_role_exists',
                    'status': 'pass',
                    'message': f"Redshift role {role_name} exists"
                })
            except ClientError as e:
                validation_results['checks'].append({
                    'check': 'redshift_role_exists',
                    'status': 'fail',
                    'message': f"Redshift role not found: {e}"
                })
                validation_results['overall_status'] = 'failed'
        
        # Check 3: Airflow user
        if 'airflow_user' in iam_results:
            user_name = iam_results['airflow_user']['user_name']
            try:
                self.iam_client.get_user(UserName=user_name)
                validation_results['checks'].append({
                    'check': 'airflow_user_exists',
                    'status': 'pass',
                    'message': f"Airflow user {user_name} exists"
                })
            except ClientError as e:
                validation_results['checks'].append({
                    'check': 'airflow_user_exists',
                    'status': 'fail',
                    'message': f"Airflow user not found: {e}"
                })
                validation_results['overall_status'] = 'failed'
        
        # Check 4: Meta Pipeline policy
        if 'meta_kaggle_pipeline_policy' in iam_results:
            policy_arn = iam_results['meta_kaggle_pipeline_policy']['policy_arn']
            try:
                self.iam_client.get_policy(PolicyArn=policy_arn)
                validation_results['checks'].append({
                    'check': 'meta_pipeline_policy_exists',
                    'status': 'pass',
                    'message': "Meta Pipeline policy exists"
                })
            except ClientError as e:
                validation_results['checks'].append({
                    'check': 'meta_pipeline_policy_exists',
                    'status': 'fail',
                    'message': f"Meta Pipeline policy not found: {e}"
                })
        
        return validation_results
    
    def validate_glue_resources(self) -> Dict[str, Any]:
        """Validate Glue databases, crawlers, jobs"""
        self.logger.info("🔍 Validating Glue resources...")
        validation_results = {
            'service': 'Glue',
            'checks': [],
            'overall_status': 'success'
        }
        
        if 'glue' not in self.deployment_results:
            validation_results['overall_status'] = 'skipped'
            return validation_results
        
        # Check 1: Databases
        expected_databases = ['meta_bronze', 'meta_silver', 'meta_gold']
        for db_name in expected_databases:
            try:
                self.glue_client.get_database(Name=db_name)
                validation_results['checks'].append({
                    'check': f'database_{db_name}_exists',
                    'status': 'pass',
                    'message': f"Database {db_name} exists"
                })
            except ClientError as e:
                validation_results['checks'].append({
                    'check': f'database_{db_name}_exists',
                    'status': 'fail',
                    'message': f"Database {db_name} not found: {e}"
                })
                validation_results['overall_status'] = 'failed'
        
        # Check 2: Crawlers count
        try:
            response = self.glue_client.list_crawlers()
            meta_crawlers = [c for c in response['CrawlerNames'] if c.startswith('meta_')]
            validation_results['checks'].append({
                'check': 'crawlers_count',
                'status': 'pass' if len(meta_crawlers) > 0 else 'warning',
                'message': f"Found {len(meta_crawlers)} Meta crawlers"
            })
        except ClientError as e:
            validation_results['checks'].append({
                'check': 'crawlers_count',
                'status': 'error',
                'message': f"Could not list crawlers: {e}"
            })
        
        # Check 3: Athena workgroup
        if 'athena' in self.deployment_results.get('glue', {}):
            workgroup_name = self.deployment_results['glue']['athena']['workgroup_name']
            try:
                self.athena_client.get_work_group(WorkGroup=workgroup_name)
                validation_results['checks'].append({
                    'check': 'athena_workgroup_exists',
                    'status': 'pass',
                    'message': f"Athena workgroup {workgroup_name} exists"
                })
            except ClientError as e:
                validation_results['checks'].append({
                    'check': 'athena_workgroup_exists',
                    'status': 'fail',
                    'message': f"Athena workgroup not found: {e}"
                })
        
        return validation_results
    
    def validate_redshift_resources(self) -> Dict[str, Any]:
        """Validate Redshift Serverless resources"""
        self.logger.info("🔍 Validating Redshift resources...")
        validation_results = {
            'service': 'RedshiftServerless',
            'checks': [],
            'overall_status': 'success'
        }
        
        if 'redshift' not in self.deployment_results:
            validation_results['overall_status'] = 'skipped'
            return validation_results
        
        redshift_results = self.deployment_results['redshift']
        
        # Check 1: Namespace
        if 'namespace' in redshift_results:
            namespace_name = redshift_results['namespace']['namespace_name']
            try:
                response = self.redshift_serverless_client.get_namespace(namespaceName=namespace_name)
                status = response['namespace']['status']
                validation_results['checks'].append({
                    'check': 'namespace_exists',
                    'status': 'pass' if status == 'AVAILABLE' else 'warning',
                    'message': f"Namespace {namespace_name} status: {status}"
                })
            except ClientError as e:
                validation_results['checks'].append({
                    'check': 'namespace_exists',
                    'status': 'fail',
                    'message': f"Namespace not found: {e}"
                })
                validation_results['overall_status'] = 'failed'
        
        # Check 2: Workgroup
        if 'workgroup' in redshift_results:
            workgroup_name = redshift_results['workgroup']['workgroup_name']
            try:
                response = self.redshift_serverless_client.get_workgroup(workgroupName=workgroup_name)
                status = response['workgroup']['status']
                validation_results['checks'].append({
                    'check': 'workgroup_exists',
                    'status': 'pass' if status == 'AVAILABLE' else 'warning',
                    'message': f"Workgroup {workgroup_name} status: {status}"
                })
                
                # Check endpoint availability
                endpoint = response['workgroup'].get('endpoint')
                if endpoint and endpoint.get('address'):
                    validation_results['checks'].append({
                        'check': 'workgroup_endpoint',
                        'status': 'pass',
                        'message': f"Workgroup endpoint available: {endpoint['address']}"
                    })
                else:
                    validation_results['checks'].append({
                        'check': 'workgroup_endpoint',
                        'status': 'warning',
                        'message': "Workgroup endpoint not yet available"
                    })
                    
            except ClientError as e:
                validation_results['checks'].append({
                    'check': 'workgroup_exists',
                    'status': 'fail',
                    'message': f"Workgroup not found: {e}"
                })
                validation_results['overall_status'] = 'failed'
        
        # Check 3: Usage limits
        if 'usage_limits' in redshift_results:
            usage_limits = redshift_results['usage_limits']['usage_limits']
            validation_results['checks'].append({
                'check': 'usage_limits',
                'status': 'pass' if len(usage_limits) > 0 else 'warning',
                'message': f"Found {len(usage_limits)} usage limits configured"
            })
        
        return validation_results
    
    def test_connectivity(self) -> Dict[str, Any]:
        """Test connectivity và basic functionality"""
        self.logger.info("🔍 Testing connectivity...")
        connectivity_results = {
            'service': 'Connectivity',
            'tests': [],
            'overall_status': 'success'
        }
        
        # Test 1: S3 access
        bucket_name = self.config['s3_bucket_name']
        try:
            # Test list objects
            self.s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
            connectivity_results['tests'].append({
                'test': 's3_access',
                'status': 'pass',
                'message': f"S3 bucket {bucket_name} is accessible"
            })
        except ClientError as e:
            connectivity_results['tests'].append({
                'test': 's3_access',
                'status': 'fail',
                'message': f"S3 access failed: {e}"
            })
            connectivity_results['overall_status'] = 'failed'
        
        # Test 2: Current user permissions
        try:
            response = self.sts_client.get_caller_identity()
            user_arn = response['Arn']
            connectivity_results['tests'].append({
                'test': 'aws_credentials',
                'status': 'pass',
                'message': f"AWS credentials valid for: {user_arn}"
            })
        except ClientError as e:
            connectivity_results['tests'].append({
                'test': 'aws_credentials',
                'status': 'fail',
                'message': f"AWS credentials test failed: {e}"
            })
            connectivity_results['overall_status'] = 'failed'
        
        # Test 3: Glue permissions (nếu có)
        if 'glue' in self.deployment_results:
            try:
                self.glue_client.list_databases(MaxResults=1)
                connectivity_results['tests'].append({
                    'test': 'glue_access',
                    'status': 'pass',
                    'message': "Glue service is accessible"
                })
            except ClientError as e:
                connectivity_results['tests'].append({
                    'test': 'glue_access',
                    'status': 'fail',
                    'message': f"Glue access failed: {e}"
                })
        
        # Test 4: Redshift permissions (nếu có)
        if 'redshift' in self.deployment_results:
            try:
                self.redshift_serverless_client.list_namespaces(MaxResults=1)
                connectivity_results['tests'].append({
                    'test': 'redshift_access',
                    'status': 'pass',
                    'message': "Redshift Serverless service is accessible"
                })
            except ClientError as e:
                connectivity_results['tests'].append({
                    'test': 'redshift_access',
                    'status': 'fail',
                    'message': f"Redshift access failed: {e}"
                })
        
        return connectivity_results
    
    def run_comprehensive_validation(self) -> Dict[str, Any]:
        """Run all validation checks"""
        self.logger.info("🧪 Running comprehensive validation...")
        
        validation_report = {
            'timestamp': time.time(),
            'environment': self.config.get('environment', 'dev'),
            'region': self.config.get('aws_region'),
            'overall_status': 'success',
            'results': {}
        }
        
        # Run all validations
        validations = [
            ('s3', self.validate_s3_resources),
            ('iam', self.validate_iam_resources),
            ('glue', self.validate_glue_resources),
            ('redshift', self.validate_redshift_resources),
            ('connectivity', self.test_connectivity)
        ]
        
        failed_services = []
        
        for service_name, validation_func in validations:
            try:
                result = validation_func()
                validation_report['results'][service_name] = result
                
                if result['overall_status'] == 'failed':
                    failed_services.append(service_name)
                    
            except Exception as e:
                self.logger.error(f"Validation failed for {service_name}: {e}")
                validation_report['results'][service_name] = {
                    'service': service_name,
                    'overall_status': 'error',
                    'error': str(e)
                }
                failed_services.append(service_name)
        
        # Set overall status
        if failed_services:
            validation_report['overall_status'] = 'failed'
            validation_report['failed_services'] = failed_services
        
        # Generate summary
        validation_report['summary'] = self._generate_validation_summary(validation_report)
        
        return validation_report
    
    def _generate_validation_summary(self, report: Dict[str, Any]) -> Dict[str, Any]:
        """Generate validation summary"""
        total_checks = 0
        passed_checks = 0
        failed_checks = 0
        warning_checks = 0
        
        for service_name, result in report['results'].items():
            if 'checks' in result:
                for check in result['checks']:
                    total_checks += 1
                    if check['status'] == 'pass':
                        passed_checks += 1
                    elif check['status'] == 'fail':
                        failed_checks += 1
                    elif check['status'] == 'warning':
                        warning_checks += 1
            
            if 'tests' in result:
                for test in result['tests']:
                    total_checks += 1
                    if test['status'] == 'pass':
                        passed_checks += 1
                    elif test['status'] == 'fail':
                        failed_checks += 1
        
        return {
            'total_checks': total_checks,
            'passed_checks': passed_checks,
            'failed_checks': failed_checks,
            'warning_checks': warning_checks,
            'success_rate': passed_checks / total_checks if total_checks > 0 else 0
        }
    
    def print_validation_report(self, report: Dict[str, Any]) -> None:
        """Print validation report in readable format"""
        print(f"\n🧪 VALIDATION REPORT")
        print(f"{'='*60}")
        print(f"Environment: {report['environment']}")
        print(f"Region: {report['region']}")
        print(f"Overall Status: {'✅ PASS' if report['overall_status'] == 'success' else '❌ FAIL'}")
        
        summary = report['summary']
        print(f"Success Rate: {summary['success_rate']:.1%} ({summary['passed_checks']}/{summary['total_checks']})")
        
        for service_name, result in report['results'].items():
            status_icon = {
                'success': '✅',
                'failed': '❌',
                'skipped': '⏭️',
                'error': '💥'
            }.get(result['overall_status'], '❓')
            
            print(f"\n{status_icon} {service_name.upper()} Service:")
            
            # Print checks
            if 'checks' in result:
                for check in result['checks']:
                    check_icon = {
                        'pass': '  ✓',
                        'fail': '  ✗',
                        'warning': '  ⚠',
                        'error': '  💥'
                    }.get(check['status'], '  ?')
                    print(f"{check_icon} {check['check']}: {check['message']}")
            
            # Print tests
            if 'tests' in result:
                for test in result['tests']:
                    test_icon = {
                        'pass': '  ✓',
                        'fail': '  ✗',
                        'warning': '  ⚠',
                        'error': '  💥'
                    }.get(test['status'], '  ?')
                    print(f"{test_icon} {test['test']}: {test['message']}")
            
            if result['overall_status'] == 'skipped':
                print("  ⏭️ Service not deployed - skipping validation")
        
        print(f"\n{'='*60}")


def run_validation_from_cli():
    """Run validation from command line"""
    import argparse
    import json
    
    parser = argparse.ArgumentParser(description="Validate Meta Kaggle Pipeline deployment")
    parser.add_argument('--environment', '-e', default='dev', help='Environment to validate')
    parser.add_argument('--deployment-file', '-f', help='JSON file với deployment results')
    parser.add_argument('--config-dir', help='Config directory path')
    
    args = parser.parse_args()
    
    # Load deployment results
    if args.deployment_file and Path(args.deployment_file).exists():
        with open(args.deployment_file, 'r') as f:
            deployment_results = json.load(f)
    else:
        deployment_results = {}
    
    # Load config
    from config.config_manager import ConfigurationManager
    config_manager = ConfigurationManager(args.config_dir)
    config = config_manager.load_config(args.environment)
    
    # Run validation
    validator = DeploymentValidator(config, deployment_results)
    report = validator.run_comprehensive_validation()
    validator.print_validation_report(report)
    
    # Save report
    report_file = f"validation_report_{args.environment}_{int(time.time())}.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    print(f"\n📄 Full report saved to: {report_file}")
    
    # Exit with appropriate code
    if report['overall_status'] == 'success':
        print("🎉 All validations passed!")
        return 0
    else:
        print("💥 Some validations failed!")
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(run_validation_from_cli())