#!/usr/bin/env python3
"""
Resource Inspector cho Meta Kaggle Pipeline Boto3 Deployment
List và check tất cả AWS resources mà không modify gì
"""

import argparse
import json
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


class ResourceInspector:
    """
    Inspector để list và check AWS resources
    """
    
    def __init__(self, environment: str = "dev", config_dir: Optional[str] = None):
        self.environment = environment
        
        # Load configuration
        self.config_manager = ConfigurationManager(config_dir)
        self.config = self.config_manager.load_config(environment)
        
        # Initialize managers
        self._initialize_managers()
    
    def _initialize_managers(self) -> None:
        """Initialize all resource managers"""
        self.s3_manager = S3Manager(self.config)
        self.iam_manager = IAMManager(self.config)
        self.glue_manager = GlueManager(self.config)
        self.redshift_manager = RedshiftManager(self.config)
    
    def inspect_s3_resources(self) -> Dict[str, Any]:
        """Inspect S3 resources"""
        results = {"exists": False, "details": {}}
        
        try:
            bucket_name = self.config['s3_bucket_name']
            
            # Check bucket exists
            if self.s3_manager.bucket_exists():
                results["exists"] = True
                results["details"]["bucket_name"] = bucket_name
                
                # Get bucket info
                response = self.s3_manager.s3_client.get_bucket_location(Bucket=bucket_name)
                results["details"]["region"] = response.get('LocationConstraint', 'us-east-1')
                
                # Count objects
                paginator = self.s3_manager.s3_client.get_paginator('list_objects_v2')
                object_count = 0
                total_size = 0
                
                for page in paginator.paginate(Bucket=bucket_name):
                    if 'Contents' in page:
                        object_count += len(page['Contents'])
                        total_size += sum(obj['Size'] for obj in page['Contents'])
                
                results["details"]["object_count"] = object_count
                results["details"]["total_size_mb"] = round(total_size / (1024*1024), 2)
                
                # Check versioning
                versioning = self.s3_manager.s3_client.get_bucket_versioning(Bucket=bucket_name)
                results["details"]["versioning"] = versioning.get('Status', 'Disabled')
                
        except Exception as e:
            results["error"] = str(e)
        
        return results
    
    def inspect_iam_resources(self) -> Dict[str, Any]:
        """Inspect IAM resources"""
        results = {"roles": [], "policies": [], "users": []}
        
        try:
            current_user_info = self.iam_manager._get_current_user_info()
            account_id = current_user_info.get('account_id')
            
            # Check roles
            role_names = ['glue-execution-role', 'redshift-service-role']
            for role_base in role_names:
                role_name = self.iam_manager.format_resource_name(role_base)
                try:
                    response = self.iam_manager.iam_client.get_role(RoleName=role_name)
                    results["roles"].append({
                        "name": role_name,
                        "arn": response['Role']['Arn'],
                        "created": response['Role']['CreateDate'].isoformat()
                    })
                except Exception:
                    results["roles"].append({"name": role_name, "status": "not_found"})
            
            # Check policies
            policy_bases = ['glue-s3-access-policy', 'redshift-s3-copy-policy', 'airflow-orchestration-policy', 'meta-kaggle-pipeline-policy']
            for policy_base in policy_bases:
                policy_name = self.iam_manager.format_resource_name(policy_base)
                policy_arn = f"arn:aws:iam::{account_id}:policy/{policy_name}"
                try:
                    response = self.iam_manager.iam_client.get_policy(PolicyArn=policy_arn)
                    results["policies"].append({
                        "name": policy_name,
                        "arn": policy_arn,
                        "versions": response['Policy']['DefaultVersionId']
                    })
                except Exception:
                    results["policies"].append({"name": policy_name, "status": "not_found"})
            
            # Check Airflow user
            airflow_user_name = self.iam_manager.format_resource_name("airflow", "user")
            try:
                response = self.iam_manager.iam_client.get_user(UserName=airflow_user_name)
                results["users"].append({
                    "name": airflow_user_name,
                    "arn": response['User']['Arn'],
                    "created": response['User']['CreateDate'].isoformat()
                })
            except Exception:
                results["users"].append({"name": airflow_user_name, "status": "not_found"})
            
        except Exception as e:
            results["error"] = str(e)
        
        return results
    
    def inspect_glue_resources(self) -> Dict[str, Any]:
        """Inspect Glue resources"""
        results = {"databases": [], "crawlers": [], "jobs": [], "athena_workgroup": {}}
        
        try:
            # Check databases
            for db_name in ['meta_bronze', 'meta_silver', 'meta_gold']:
                try:
                    response = self.glue_manager.glue_client.get_database(Name=db_name)
                    results["databases"].append({
                        "name": db_name,
                        "description": response['Database'].get('Description', ''),
                        "created": response['Database']['CreateTime'].isoformat()
                    })
                except Exception:
                    results["databases"].append({"name": db_name, "status": "not_found"})
            
            # Check crawlers
            try:
                response = self.glue_manager.glue_client.list_crawlers()
                meta_crawlers = [name for name in response.get('CrawlerNames', []) 
                               if name.startswith('meta_')]
                results["crawlers"] = meta_crawlers
            except Exception as e:
                results["crawlers"] = {"error": str(e)}
            
            # Check jobs
            try:
                response = self.glue_manager.glue_client.list_jobs()
                meta_jobs = [name for name in response.get('JobNames', [])
                           if name.startswith('meta_')]
                results["jobs"] = meta_jobs
            except Exception as e:
                results["jobs"] = {"error": str(e)}
            
            # Check Athena workgroup
            try:
                response = self.glue_manager.athena_client.get_work_group(
                    WorkGroup=self.glue_manager.athena_workgroup_name
                )
                results["athena_workgroup"] = {
                    "name": self.glue_manager.athena_workgroup_name,
                    "state": response['WorkGroup']['State'],
                    "created": response['WorkGroup']['CreationTime'].isoformat()
                }
            except Exception:
                results["athena_workgroup"] = {
                    "name": self.glue_manager.athena_workgroup_name, 
                    "status": "not_found"
                }
            
        except Exception as e:
            results["error"] = str(e)
        
        return results
    
    def inspect_redshift_resources(self) -> Dict[str, Any]:
        """Inspect Redshift resources"""
        results = {"namespace": {}, "workgroup": {}, "usage_limits": []}
        
        try:
            # Check namespace
            try:
                response = self.redshift_manager.redshift_serverless_client.get_namespace(
                    namespaceName=self.redshift_manager.namespace_name
                )
                namespace = response['namespace']
                results["namespace"] = {
                    "name": namespace['namespaceName'],
                    "status": namespace['status'],
                    "created": namespace['creationDate'].isoformat(),
                    "database": namespace['dbName']
                }
            except Exception:
                results["namespace"] = {
                    "name": self.redshift_manager.namespace_name,
                    "status": "not_found"
                }
            
            # Check workgroup
            try:
                response = self.redshift_manager.redshift_serverless_client.get_workgroup(
                    workgroupName=self.redshift_manager.workgroup_name
                )
                workgroup = response['workgroup']
                results["workgroup"] = {
                    "name": workgroup['workgroupName'],
                    "status": workgroup['status'],
                    "created": workgroup['creationDate'].isoformat(),
                    "base_capacity": workgroup.get('baseCapacity'),
                    "endpoint": workgroup.get('endpoint', {}).get('address', 'N/A')
                }
            except Exception:
                results["workgroup"] = {
                    "name": self.redshift_manager.workgroup_name,
                    "status": "not_found"
                }
            
            # Check usage limits
            try:
                response = self.redshift_manager.redshift_serverless_client.list_usage_limits()
                for limit in response.get('usageLimits', []):
                    if self.redshift_manager.workgroup_name in limit.get('resourceArn', ''):
                        results["usage_limits"].append({
                            "id": limit['usageLimitId'],
                            "amount": limit['amount'],
                            "period": limit['period'],
                            "breach_action": limit['breachAction']
                        })
            except Exception as e:
                results["usage_limits"] = {"error": str(e)}
            
        except Exception as e:
            results["error"] = str(e)
        
        return results
    
    def generate_full_report(self) -> Dict[str, Any]:
        """Generate complete resource inspection report"""
        print(f"\n🔍 Meta Kaggle Pipeline Resource Inspector")
        print(f"Environment: {self.environment}")
        print(f"Region: {self.config['aws_region']}")
        print("="*80)
        
        report = {
            "environment": self.environment,
            "region": self.config['aws_region'],
            "timestamp": str(Path().resolve()),
            "resources": {}
        }
        
        # Inspect each service
        print("\n📦 S3 Resources:")
        s3_results = self.inspect_s3_resources()
        report["resources"]["s3"] = s3_results
        if s3_results["exists"]:
            details = s3_results["details"]
            print(f"  ✅ Bucket: {details['bucket_name']}")
            print(f"     Objects: {details['object_count']}")
            print(f"     Size: {details['total_size_mb']} MB")
            print(f"     Versioning: {details['versioning']}")
        else:
            print("  ❌ S3 bucket not found")
        
        print("\n🔐 IAM Resources:")
        iam_results = self.inspect_iam_resources()
        report["resources"]["iam"] = iam_results
        
        roles_found = len([r for r in iam_results["roles"] if "arn" in r])
        policies_found = len([p for p in iam_results["policies"] if "arn" in p]) 
        users_found = len([u for u in iam_results["users"] if "arn" in u])
        
        print(f"  Roles: {roles_found}/{len(iam_results['roles'])}")
        print(f"  Policies: {policies_found}/{len(iam_results['policies'])}")
        print(f"  Users: {users_found}/{len(iam_results['users'])}")
        
        print("\n🕷️ Glue Resources:")
        glue_results = self.inspect_glue_resources()
        report["resources"]["glue"] = glue_results
        
        dbs_found = len([d for d in glue_results["databases"] if "created" in d])
        print(f"  Databases: {dbs_found}/3")
        if isinstance(glue_results["crawlers"], list):
            print(f"  Crawlers: {len(glue_results['crawlers'])}")
        if isinstance(glue_results["jobs"], list):
            print(f"  Jobs: {len(glue_results['jobs'])}")
        
        athena_status = "✅ Found" if "created" in glue_results["athena_workgroup"] else "❌ Not found"
        print(f"  Athena Workgroup: {athena_status}")
        
        print("\n🏢 Redshift Serverless:")
        redshift_results = self.inspect_redshift_resources()
        report["resources"]["redshift"] = redshift_results
        
        namespace_status = "✅ Found" if "created" in redshift_results["namespace"] else "❌ Not found"
        workgroup_status = "✅ Found" if "created" in redshift_results["workgroup"] else "❌ Not found"
        
        print(f"  Namespace: {namespace_status}")
        print(f"  Workgroup: {workgroup_status}")
        if isinstance(redshift_results["usage_limits"], list):
            print(f"  Usage Limits: {len(redshift_results['usage_limits'])}")
        
        if "created" in redshift_results["workgroup"]:
            endpoint = redshift_results["workgroup"]["endpoint"]
            if endpoint != "N/A":
                print(f"  JDBC URL: jdbc:redshift://{endpoint}:5439/{redshift_results['namespace'].get('database', 'meta_warehouse')}")
        
        print("\n" + "="*80)
        
        return report


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(description="Inspect Meta Kaggle Pipeline Resources")
    parser.add_argument("--environment", default="dev", help="Environment to inspect (default: dev)")
    parser.add_argument("--config-dir", help="Custom configuration directory")
    parser.add_argument("--output-file", help="Save report to JSON file")
    parser.add_argument("--service", choices=['s3', 'iam', 'glue', 'redshift'], 
                       help="Inspect specific service only")
    
    args = parser.parse_args()
    
    try:
        inspector = ResourceInspector(args.environment, args.config_dir)
        
        if args.service:
            # Inspect specific service
            if args.service == 's3':
                results = inspector.inspect_s3_resources()
            elif args.service == 'iam':
                results = inspector.inspect_iam_resources()
            elif args.service == 'glue':
                results = inspector.inspect_glue_resources()
            elif args.service == 'redshift':
                results = inspector.inspect_redshift_resources()
            
            print(json.dumps(results, indent=2, default=str))
        else:
            # Full inspection
            report = inspector.generate_full_report()
            
            if args.output_file:
                with open(args.output_file, 'w') as f:
                    json.dump(report, f, indent=2, default=str)
                print(f"\n📄 Report saved to: {args.output_file}")
    
    except Exception as e:
        print(f"💥 Inspection failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()