#!/usr/bin/env python3
"""
Destroy Script cho Meta Kaggle Pipeline Boto3 Deployment
Safely remove all AWS resources created by the deployment system
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
from managers.s3_manager import S3Manager
from managers.iam_manager import IAMManager
from managers.glue_manager import GlueManager
from managers.redshift_manager import RedshiftManager


class MetaKagglePipelineDestroyer:
    """
    Main destroyer cho Meta Kaggle Pipeline resources
    Safely removes all AWS resources in reverse order
    """
    
    def __init__(self, environment: str = "dev", config_dir: Optional[str] = None):
        self.environment = environment
        self.logger = self._setup_logger()
        
        # Load configuration
        self.config_manager = ConfigurationManager(config_dir)
        self.config = self.config_manager.load_config(environment)
        
        # Initialize managers
        self._initialize_managers()
        
        self.logger.info(f"Initialized destroyer for environment: {environment}")
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logging cho destroyer"""
        logger = logging.getLogger('meta_pipeline_destroyer')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _initialize_managers(self) -> None:
        """Initialize all resource managers"""
        try:
            self.s3_manager = S3Manager(self.config)
            self.iam_manager = IAMManager(self.config)
            self.glue_manager = GlueManager(self.config)
            self.redshift_manager = RedshiftManager(self.config)
            
            self.logger.info("Initialized 4 managers for destruction")
        except Exception as e:
            self.logger.error(f"Failed to initialize managers: {e}")
            raise
    
    def print_header(self) -> None:
        """Print destruction header"""
        print("\n" + "="*80)
        print("🗑️  META KAGGLE PIPELINE DESTRUCTION (Boto3)")
        print("⚠️  WARNING: This will permanently delete AWS resources!")
        print(f"Environment: {self.environment}")
        print(f"Region: {self.config['aws_region']}")
        print("="*80 + "\n")
    
    def confirm_destruction(self) -> bool:
        """Confirm user wants to proceed with destruction"""
        print("⚠️  DESTRUCTION CONFIRMATION REQUIRED ⚠️")
        print("\nThis action will permanently delete the following resources:")
        print("  • Redshift Serverless namespace and workgroup")
        print("  • Glue databases, crawlers, jobs, and Athena workgroup")
        print("  • IAM roles, policies, and users")
        print("  • S3 bucket and ALL data (if --delete-s3-data flag is used)")
        print("\n💀 THIS ACTION CANNOT BE UNDONE!")
        
        confirmation = input("\nType 'DELETE' to confirm destruction: ")
        return confirmation == "DELETE"
    
    def load_deployment_state(self, deployment_file: Optional[str] = None) -> Dict[str, Any]:
        """Load deployment state từ JSON file"""
        if deployment_file and Path(deployment_file).exists():
            with open(deployment_file, 'r') as f:
                return json.load(f)
        
        # Find latest deployment file
        logs_dir = Path(__file__).parent / "logs"
        if logs_dir.exists():
            deployment_files = list(logs_dir.glob(f"deployment_state_{self.environment}_*.json"))
            if deployment_files:
                latest_file = max(deployment_files, key=lambda x: x.stat().st_mtime)
                self.logger.info(f"Loading deployment state from: {latest_file}")
                with open(latest_file, 'r') as f:
                    return json.load(f)
        
        self.logger.warning("No deployment state found, will attempt to discover resources")
        return {}
    
    def destroy_redshift_resources(self, deployment_state: Dict[str, Any]) -> Dict[str, Any]:
        """Destroy Redshift Serverless resources"""
        self.logger.info("🏢 Destroying Redshift Serverless resources...")
        results = {"deleted_resources": [], "errors": []}
        
        try:
            # Get role ARN from deployment state or IAM
            redshift_role_arn = deployment_state.get('iam', {}).get('redshift_role_arn')
            if redshift_role_arn:
                self.redshift_manager.set_redshift_role_arn(redshift_role_arn)
            
            # Delete usage limits first
            try:
                usage_limits_data = deployment_state.get('redshift', {}).get('usage_limits', {})
                usage_limits = usage_limits_data.get('usage_limits', [])
                
                for limit in usage_limits:
                    try:
                        self.redshift_manager.redshift_serverless_client.delete_usage_limit(
                            usageLimitId=limit['usage_limit_id']
                        )
                        results["deleted_resources"].append(f"Usage limit: {limit['usage_limit_id']}")
                        self.logger.info(f"✓ Deleted usage limit: {limit['usage_limit_id']}")
                    except Exception as e:
                        # Check if it's a permission error and skip gracefully
                        if "AccessDenied" in str(e) or "not authorized" in str(e):
                            self.logger.warning(f"Permission denied for usage limit deletion: {limit['usage_limit_id']}")
                        else:
                            self.logger.warning(f"Could not delete usage limit {limit['usage_limit_id']}: {e}")
            except Exception as e:
                self.logger.warning(f"Error deleting usage limits: {e}")
            
            # Delete workgroup
            try:
                workgroup_result = self.redshift_manager.delete_workgroup()
                if workgroup_result and workgroup_result.get('status') == 'deleted':
                    results["deleted_resources"].append(f"Workgroup: {self.redshift_manager.workgroup_name}")
            except Exception as e:
                results["errors"].append(f"Workgroup deletion: {str(e)}")
            
            # Delete namespace
            try:
                namespace_result = self.redshift_manager.delete_namespace()
                if namespace_result and namespace_result.get('status') == 'deleted':
                    results["deleted_resources"].append(f"Namespace: {self.redshift_manager.namespace_name}")
            except Exception as e:
                results["errors"].append(f"Namespace deletion: {str(e)}")
        
        except Exception as e:
            # Check if it's a permission error and provide helpful message
            error_msg = str(e)
            if "AccessDenied" in error_msg or "not authorized" in error_msg:
                results["skipped_resources"] = results.get("skipped_resources", [])
                results["skipped_resources"].append("Redshift resources (permission denied)")
                self.logger.warning("⚠️ Skipping Redshift resources due to insufficient permissions")
                self.logger.info("💡 Run 'python permission_checker.py' to see required permissions")
            else:
                self.logger.error(f"❌ Redshift destruction failed: {e}")
                results["errors"].append(str(e))
        
        return results
    
    def destroy_glue_resources(self, deployment_state: Dict[str, Any]) -> Dict[str, Any]:
        """Destroy Glue Data Catalog resources"""
        self.logger.info("🕷️ Destroying Glue resources...")
        results = {"deleted_resources": [], "errors": []}
        
        try:
            # Get glue role from deployment state
            glue_role_arn = deployment_state.get('iam', {}).get('glue_role_arn')
            if glue_role_arn:
                self.glue_manager.set_glue_role_arn(glue_role_arn)
            
            # Delete Athena workgroup
            try:
                self.glue_manager.athena_client.delete_work_group(
                    WorkGroup=self.glue_manager.athena_workgroup_name,
                    RecursiveDeleteOption=True
                )
                results["deleted_resources"].append(f"Athena workgroup: {self.glue_manager.athena_workgroup_name}")
                self.logger.info(f"✓ Deleted Athena workgroup: {self.glue_manager.athena_workgroup_name}")
            except Exception as e:
                self.logger.warning(f"Could not delete Athena workgroup: {e}")
            
            # Delete Glue jobs
            for table in self.glue_manager.tables:
                for layer in ['bronze', 'silver']:
                    job_name = f"meta_{layer}_{table}_transform"
                    try:
                        self.glue_manager.glue_client.delete_job(JobName=job_name)
                        results["deleted_resources"].append(f"Glue job: {job_name}")
                        self.logger.info(f"✓ Deleted Glue job: {job_name}")
                    except Exception as e:
                        self.logger.warning(f"Could not delete job {job_name}: {e}")
            
            # Delete crawlers
            crawler_prefixes = ['meta_bronze_', 'meta_silver_', 'meta_gold_']
            for prefix in crawler_prefixes:
                try:
                    response = self.glue_manager.glue_client.list_crawlers(
                        NextToken='', MaxResults=100
                    )
                    for crawler_name in response.get('CrawlerNames', []):
                        if crawler_name.startswith(prefix):
                            try:
                                self.glue_manager.glue_client.delete_crawler(Name=crawler_name)
                                results["deleted_resources"].append(f"Crawler: {crawler_name}")
                                self.logger.info(f"✓ Deleted crawler: {crawler_name}")
                            except Exception as e:
                                self.logger.warning(f"Could not delete crawler {crawler_name}: {e}")
                except Exception as e:
                    self.logger.warning(f"Error listing crawlers: {e}")
            
            # Delete databases
            for db_name in ['meta_bronze', 'meta_silver', 'meta_gold']:
                try:
                    self.glue_manager.glue_client.delete_database(Name=db_name)
                    results["deleted_resources"].append(f"Database: {db_name}")
                    self.logger.info(f"✓ Deleted database: {db_name}")
                except Exception as e:
                    self.logger.warning(f"Could not delete database {db_name}: {e}")
        
        except Exception as e:
            # Check if it's a permission error and provide helpful message
            error_msg = str(e)
            if "AccessDenied" in error_msg or "not authorized" in error_msg:
                results["skipped_resources"] = results.get("skipped_resources", [])
                results["skipped_resources"].append("Glue resources (permission denied)")
                self.logger.warning("⚠️ Skipping Glue resources due to insufficient permissions")
                self.logger.info("💡 Run 'python permission_checker.py' to see required permissions")
            else:
                self.logger.error(f"❌ Glue destruction failed: {e}")
                results["errors"].append(str(e))
        
        return results
    
    def destroy_iam_resources(self, deployment_state: Dict[str, Any]) -> Dict[str, Any]:
        """Destroy IAM resources"""
        self.logger.info("🔐 Destroying IAM resources...")
        results = {"deleted_resources": [], "errors": []}
        
        try:
            # Get current user info
            current_user_info = self.iam_manager._get_current_user_info()
            
            # Detach policies from current user first
            policy_name = self.iam_manager.format_resource_name("meta-kaggle-pipeline", "policy")
            policy_arn = f"arn:aws:iam::{current_user_info.get('account_id')}:policy/{policy_name}"
            
            try:
                self.iam_manager._detach_policy_from_user(policy_arn, current_user_info.get('username'))
                self.logger.info(f"✓ Detached policy from user: {current_user_info.get('username')}")
            except Exception as e:
                # Skip if no policy or user issues  
                error_msg = str(e)
                if 'NoSuchEntity' in error_msg or 'not found' in error_msg or 'Invalid length' in error_msg:
                    self.logger.warning(f"Policy detachment skipped (may already be done)")
                else:
                    self.logger.warning(f"Could not detach policy from user: {e}")
            
            # Delete Airflow user access keys and user
            airflow_user_name = self.iam_manager.format_resource_name("airflow", "user")
            try:
                # List and delete access keys
                access_keys = self.iam_manager.iam_client.list_access_keys(UserName=airflow_user_name)
                for key in access_keys['AccessKeyMetadata']:
                    self.iam_manager.iam_client.delete_access_key(
                        UserName=airflow_user_name,
                        AccessKeyId=key['AccessKeyId']
                    )
                
                # Delete user
                self.iam_manager.iam_client.delete_user(UserName=airflow_user_name)
                results["deleted_resources"].append(f"User: {airflow_user_name}")
                self.logger.info(f"✓ Deleted Airflow user: {airflow_user_name}")
            except Exception as e:
                error_msg = str(e)
                if 'NoSuchEntity' in error_msg or 'cannot be found' in error_msg:
                    self.logger.info(f"Airflow user {airflow_user_name} not found (may already be deleted)")
                else:
                    self.logger.warning(f"Could not delete Airflow user: {e}")
            
            # Delete roles and their policies
            role_names = ['glue-execution-role', 'redshift-service-role']
            for role_base in role_names:
                role_name = self.iam_manager.format_resource_name(role_base)
                try:
                    # Detach managed policies
                    response = self.iam_manager.iam_client.list_attached_role_policies(RoleName=role_name)
                    for policy in response['AttachedPolicies']:
                        self.iam_manager.iam_client.detach_role_policy(
                            RoleName=role_name,
                            PolicyArn=policy['PolicyArn']
                        )
                    
                    # Delete role
                    self.iam_manager.iam_client.delete_role(RoleName=role_name)
                    results["deleted_resources"].append(f"Role: {role_name}")
                    self.logger.info(f"✓ Deleted role: {role_name}")
                except Exception as e:
                    # Check if it's a NoSuchEntity error (already deleted)
                    error_msg = str(e)
                    if 'NoSuchEntity' in error_msg or 'cannot be found' in error_msg:
                        self.logger.warning(f"Role {role_name} not found (may already be deleted)")
                    else:
                        self.logger.warning(f"Could not delete role {role_name}: {e}")
            
            # Delete custom policies
            policy_bases = ['glue-s3-access-policy', 'redshift-s3-copy-policy', 'airflow-orchestration-policy', 'meta-kaggle-pipeline-policy']
            for policy_base in policy_bases:
                policy_name = self.iam_manager.format_resource_name(policy_base)
                policy_arn = f"arn:aws:iam::{current_user_info.get('account_id')}:policy/{policy_name}"
                try:
                    # First delete all non-default versions
                    self.iam_manager._delete_policy_versions(policy_arn)
                    
                    # Then delete the policy
                    self.iam_manager.iam_client.delete_policy(PolicyArn=policy_arn)
                    results["deleted_resources"].append(f"Policy: {policy_name}")
                    self.logger.info(f"✓ Deleted policy: {policy_name}")
                except Exception as e:
                    # Check if it's a NoSuchEntity error (already deleted)
                    error_msg = str(e)
                    if 'NoSuchEntity' in error_msg or 'does not exist' in error_msg:
                        self.logger.info(f"Policy {policy_name} not found (may already be deleted)")
                    else:
                        self.logger.warning(f"Could not delete policy {policy_name}: {e}")
        
        except Exception as e:
            self.logger.error(f"❌ IAM destruction failed: {e}")
            results["errors"].append(str(e))
        
        return results
    
    def destroy_s3_resources(self, delete_data: bool = False) -> Dict[str, Any]:
        """Destroy S3 resources"""
        self.logger.info("📦 Destroying S3 resources...")
        results = {"deleted_resources": [], "errors": []}
        
        try:
            bucket_name = self.config['s3_bucket_name']
            
            if delete_data:
                # Empty bucket first
                self.logger.info(f"🗑️ Emptying bucket: {bucket_name}")
                try:
                    # Delete all objects
                    paginator = self.s3_manager.s3_client.get_paginator('list_objects_v2')
                    for page in paginator.paginate(Bucket=bucket_name):
                        if 'Contents' in page:
                            objects = [{'Key': obj['Key']} for obj in page['Contents']]
                            if objects:
                                self.s3_manager.s3_client.delete_objects(
                                    Bucket=bucket_name,
                                    Delete={'Objects': objects}
                                )
                    
                    # Delete all object versions
                    paginator = self.s3_manager.s3_client.get_paginator('list_object_versions')
                    for page in paginator.paginate(Bucket=bucket_name):
                        versions = []
                        if 'Versions' in page:
                            versions.extend([{'Key': v['Key'], 'VersionId': v['VersionId']} for v in page['Versions']])
                        if 'DeleteMarkers' in page:
                            versions.extend([{'Key': d['Key'], 'VersionId': d['VersionId']} for d in page['DeleteMarkers']])
                        
                        if versions:
                            self.s3_manager.s3_client.delete_objects(
                                Bucket=bucket_name,
                                Delete={'Objects': versions}
                            )
                    
                    # Delete bucket
                    self.s3_manager.s3_client.delete_bucket(Bucket=bucket_name)
                    results["deleted_resources"].append(f"S3 bucket (with data): {bucket_name}")
                    self.logger.info(f"✓ Deleted S3 bucket with data: {bucket_name}")
                
                except Exception as e:
                    self.logger.error(f"Could not delete S3 bucket: {e}")
                    results["errors"].append(f"S3 bucket deletion: {str(e)}")
            else:
                self.logger.info(f"ℹ️ Skipping S3 data deletion (use --delete-s3-data to delete data)")
                results["deleted_resources"].append("S3 bucket: SKIPPED (data preserved)")
        
        except Exception as e:
            self.logger.error(f"❌ S3 destruction failed: {e}")
            results["errors"].append(str(e))
        
        return results
    
    def destroy_infrastructure(self, 
                             delete_s3_data: bool = False,
                             deployment_file: Optional[str] = None) -> Dict[str, Any]:
        """Main destroy method"""
        self.print_header()
        
        if not self.confirm_destruction():
            self.logger.info("❌ Destruction cancelled by user")
            return {"status": "cancelled"}
        
        # Load deployment state
        deployment_state = self.load_deployment_state(deployment_file)
        
        # Destroy in reverse order (opposite of creation)
        destruction_results = {
            "redshift": {},
            "glue": {},
            "iam": {}, 
            "s3": {},
            "summary": {"total_deleted": 0, "total_errors": 0}
        }
        
        # 1. Destroy Redshift (most dependent)
        self.logger.info("\n" + "="*50)
        destruction_results["redshift"] = self.destroy_redshift_resources(deployment_state)
        
        # 2. Destroy Glue resources
        self.logger.info("\n" + "="*50) 
        destruction_results["glue"] = self.destroy_glue_resources(deployment_state)
        
        # 3. Destroy IAM resources
        self.logger.info("\n" + "="*50)
        destruction_results["iam"] = self.destroy_iam_resources(deployment_state)
        
        # 4. Destroy S3 (least dependent, but optional)
        self.logger.info("\n" + "="*50)
        destruction_results["s3"] = self.destroy_s3_resources(delete_s3_data)
        
        # Calculate summary
        for service_results in destruction_results.values():
            if isinstance(service_results, dict):
                destruction_results["summary"]["total_deleted"] += len(service_results.get("deleted_resources", []))
                destruction_results["summary"]["total_errors"] += len(service_results.get("errors", []))
                
        # Count skipped resources
        destruction_results["summary"]["total_skipped"] = 0
        for service_results in destruction_results.values():
            if isinstance(service_results, dict):
                destruction_results["summary"]["total_skipped"] += len(service_results.get("skipped_resources", []))
        
        # Print summary
        self._print_destruction_summary(destruction_results)
        
        return destruction_results
    
    def _print_destruction_summary(self, results: Dict[str, Any]) -> None:
        """Print destruction summary"""
        print("\n" + "="*80)
        print("📊 DESTRUCTION SUMMARY")
        print("="*80)
        print(f"Environment: {self.environment}")
        print(f"Region: {self.config['aws_region']}")
        
        total_deleted = results["summary"]["total_deleted"]
        total_errors = results["summary"]["total_errors"]
        total_skipped = results["summary"].get("total_skipped", 0)
        
        if total_deleted > 0:
            print(f"✅ Successfully deleted: {total_deleted} resources")
        if total_skipped > 0:
            print(f"⏭️ Skipped due to permissions: {total_skipped} resource groups")
        if total_errors > 0:
            print(f"❌ Errors encountered: {total_errors}")
        
        for service, service_results in results.items():
            if service == "summary":
                continue
            
            if isinstance(service_results, dict):
                deleted = service_results.get("deleted_resources", [])
                skipped = service_results.get("skipped_resources", [])
                errors = service_results.get("errors", [])
                
                print(f"\n🔧 {service.upper()}:")
                for resource in deleted:
                    print(f"  ✓ {resource}")
                for resource in skipped:
                    print(f"  ⏭️ {resource}")
                for error in errors:
                    print(f"  ❌ {error}")
        
        print("\n" + "="*80)
        if total_errors == 0 and total_skipped == 0:
            print("🎉 Destruction completed successfully!")
        elif total_errors == 0 and total_skipped > 0:
            print("✅ Destruction completed (some resources skipped due to permissions)")
            print("💡 Run 'python permission_checker.py' for required permissions")
        else:
            print("⚠️ Destruction completed with some errors")
            print("💡 Most errors are due to insufficient IAM permissions")
            print("🔧 System should auto-grant these permissions during deployment")
            print("🔧 If needed, run 'python permission_checker.py' to see required permissions")
        print("="*80)


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(description="Destroy Meta Kaggle Pipeline Infrastructure")
    parser.add_argument("--environment", default="dev", help="Environment to destroy (default: dev)")
    parser.add_argument("--config-dir", help="Custom configuration directory")
    parser.add_argument("--deployment-file", help="Specific deployment state file to use")
    parser.add_argument("--delete-s3-data", action="store_true", 
                       help="⚠️ WARNING: Delete S3 bucket and ALL data")
    parser.add_argument("--yes", action="store_true",
                       help="Skip confirmation prompt (DANGEROUS)")
    
    args = parser.parse_args()
    
    try:
        destroyer = MetaKagglePipelineDestroyer(args.environment, args.config_dir)
        
        if args.yes:
            # Override confirmation method for automated destruction
            destroyer.confirm_destruction = lambda: True
        
        results = destroyer.destroy_infrastructure(
            delete_s3_data=args.delete_s3_data,
            deployment_file=args.deployment_file
        )
        
        if results.get("status") == "cancelled":
            sys.exit(1)
        
        # Exit with error code if there were errors
        if results["summary"]["total_errors"] > 0:
            sys.exit(1)
    
    except KeyboardInterrupt:
        print("\n❌ Destruction interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"💥 Destruction failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()