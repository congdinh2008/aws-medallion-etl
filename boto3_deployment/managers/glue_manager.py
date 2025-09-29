#!/usr/bin/env python3
"""
GlueManager cho Meta Kaggle Pipeline
Tạo Glue databases, crawlers, jobs và Athena workgroup
Tương tự terraform/modules/glue/main.tf
"""

from typing import Dict, List, Any, Optional
from botocore.exceptions import ClientError

from .base_manager import BaseInfrastructureManager


class GlueManager(BaseInfrastructureManager):
    """
    Manager cho Glue Data Catalog, Crawlers và Athena
    Tương tự terraform/modules/glue/
    """
    
    def __init__(self, config: Dict[str, Any], session=None):
        """Khởi tạo GlueManager"""
        super().__init__(config, session)
        self.glue_client = self.get_client('glue')
        self.athena_client = self.get_client('athena')
        
        self.s3_bucket_name = config['s3_bucket_name']
        self.glue_role_arn = None  # Sẽ được set từ IAM deployment results
        
        # Tables và layers
        self.tables = config.get('medallion_tables', [
            'users', 'datasets', 'competitions', 'tags', 'kernels'
        ])
        
        # Dimensions và facts cho Gold layer
        self.dimensions = [
            'dim_user', 'dim_dataset', 'dim_competition', 'dim_tag', 'dim_date'
        ]
        
        self.facts = [
            'fact_dataset_owner_daily',
            'fact_competitions_yearly', 
            'fact_tag_usage_daily'
        ]
        
        self.athena_workgroup_name = config.get('athena_workgroup_name', 'meta-analytics')
        self.athena_output_location = config.get(
            'athena_output_location', 
            f"s3://{self.s3_bucket_name}/meta/athena-results/"
        )
        
        # Get current user info for ARN construction
        try:
            sts_client = self.get_client('sts')
            caller_identity = sts_client.get_caller_identity()
            self.account_id = caller_identity.get('Account', '000000000000')
        except Exception as e:
            self.logger.warning(f"Could not get account ID: {e}")
            self.account_id = '000000000000'
    
    def set_glue_role_arn(self, role_arn: str) -> None:
        """Set Glue role ARN từ IAM deployment"""
        self.glue_role_arn = role_arn
    
    def validate_config(self) -> bool:
        """Validate Glue configuration"""
        if not self.s3_bucket_name:
            raise ValueError("S3 bucket name is required for Glue configuration")
        
        if not self.glue_role_arn:
            self.logger.warning("Glue role ARN not set - will need to be provided during deployment")
        
        return True
    
    def create_glue_databases(self) -> Dict[str, List[str]]:
        """
        Tạo Glue databases cho Bronze, Silver, Gold layers
        Tương tự aws_glue_catalog_database resources
        """
        databases = {
            'bronze': f"meta_bronze",
            'silver': f"meta_silver", 
            'gold': f"meta_gold"
        }
        
        created_databases = []
        
        for layer, db_name in databases.items():
            try:
                # Check if database exists
                try:
                    self.glue_client.get_database(Name=db_name)
                    self.logger.info(f"Database {db_name} already exists")
                except ClientError as e:
                    if e.response['Error']['Code'] == 'EntityNotFoundException':
                        # Create new database
                        self.glue_client.create_database(
                            DatabaseInput={
                                'Name': db_name,
                                'Description': f"{layer.title()} layer database cho Meta Kaggle {'raw data với metadata' if layer == 'bronze' else 'cleaned và enriched data' if layer == 'silver' else 'dimensional model và business metrics'}",
                                'Parameters': {
                                    'classification': 'parquet',
                                    'compressionType': 'snappy',
                                    'layer': layer,
                                    'environment': self.environment
                                }
                            }
                        )
                        self.logger.info(f"Created Glue database: {db_name}")
                        created_databases.append(db_name)
                    else:
                        raise e
                        
            except ClientError as e:
                self.handle_aws_error(e, f"creating database {db_name}")
        
        return {"created_databases": created_databases, "all_databases": list(databases.values())}
    
    def create_bronze_crawlers(self) -> Dict[str, List[str]]:
        """
        Tạo crawlers cho Bronze layer - một crawler cho mỗi table
        Tương tự aws_glue_crawler.bronze_table_crawlers
        """
        created_crawlers = []
        
        for table in self.tables:
            crawler_name = f"meta_bronze_{table}_crawler"
            s3_target = f"s3://{self.s3_bucket_name}/meta/bronze/{table}/"
            
            try:
                # Check if crawler exists
                try:
                    self.glue_client.get_crawler(Name=crawler_name)
                    self.logger.info(f"Crawler {crawler_name} already exists")
                except ClientError as e:
                    if e.response['Error']['Code'] == 'EntityNotFoundException':
                        # Create new crawler
                        self.glue_client.create_crawler(
                            Name=crawler_name,
                            Role=self.glue_role_arn,
                            DatabaseName='meta_bronze',
                            Description=f"Crawler cho Bronze {table} table trong Meta Kaggle Pipeline",
                            Targets={
                                'S3Targets': [{
                                    'Path': s3_target,
                                    'Exclusions': ['**/_SUCCESS', '**/_temporary/**']
                                }]
                            },
                            TablePrefix=f"{table}_",
                            SchemaChangePolicy={
                                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                                'DeleteBehavior': 'LOG'
                            },
                            RecrawlPolicy={'RecrawlBehavior': 'CRAWL_EVERYTHING'},
                            Tags=self._get_crawler_tags(layer='bronze', table=table)
                        )
                        self.logger.info(f"Created Bronze crawler: {crawler_name}")
                        created_crawlers.append(crawler_name)
                    else:
                        raise e
                        
            except ClientError as e:
                self.handle_aws_error(e, f"creating Bronze crawler {crawler_name}")
        
        return {"created_crawlers": created_crawlers}
    
    def create_silver_crawlers(self) -> Dict[str, List[str]]:
        """Tạo crawlers cho Silver layer"""
        created_crawlers = []
        
        for table in self.tables:
            crawler_name = f"meta_silver_{table}_crawler"
            s3_target = f"s3://{self.s3_bucket_name}/meta/silver/{table}/"
            
            try:
                # Check if crawler exists
                try:
                    self.glue_client.get_crawler(Name=crawler_name)
                    self.logger.info(f"Crawler {crawler_name} already exists")
                except ClientError as e:
                    if e.response['Error']['Code'] == 'EntityNotFoundException':
                        # Create new crawler
                        self.glue_client.create_crawler(
                            Name=crawler_name,
                            Role=self.glue_role_arn,
                            DatabaseName='meta_silver',
                            Description=f"Crawler cho Silver {table} table",
                            Targets={
                                'S3Targets': [{
                                    'Path': s3_target,
                                    'Exclusions': ['**/_SUCCESS', '**/_temporary/**']
                                }]
                            },
                            TablePrefix=f"{table}_",
                            SchemaChangePolicy={
                                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                                'DeleteBehavior': 'LOG'
                            },
                            Tags=self._get_crawler_tags(layer='silver', table=table)
                        )
                        self.logger.info(f"Created Silver crawler: {crawler_name}")
                        created_crawlers.append(crawler_name)
                    else:
                        raise e
                        
            except ClientError as e:
                self.handle_aws_error(e, f"creating Silver crawler {crawler_name}")
        
        return {"created_crawlers": created_crawlers}
    
    def create_gold_crawlers(self) -> Dict[str, List[str]]:
        """Tạo crawlers cho Gold layer dimensions và facts"""
        created_crawlers = []
        
        # Dimension crawlers
        for dimension in self.dimensions:
            crawler_name = f"meta_gold_{dimension}_crawler"
            s3_target = f"s3://{self.s3_bucket_name}/meta/gold/{dimension}/"
            
            try:
                try:
                    self.glue_client.get_crawler(Name=crawler_name)
                    self.logger.info(f"Crawler {crawler_name} already exists")
                except ClientError as e:
                    if e.response['Error']['Code'] == 'EntityNotFoundException':
                        self.glue_client.create_crawler(
                            Name=crawler_name,
                            Role=self.glue_role_arn,
                            DatabaseName='meta_gold',
                            Description=f"Crawler cho Gold dimension {dimension}",
                            Targets={
                                'S3Targets': [{
                                    'Path': s3_target,
                                    'Exclusions': ['**/_SUCCESS', '**/_temporary/**']
                                }]
                            },
                            TablePrefix=f"{dimension}_",
                            SchemaChangePolicy={
                                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                                'DeleteBehavior': 'LOG'
                            },
                            Tags=self._get_crawler_tags(layer='gold', table=dimension, resource_type='dimension')
                        )
                        self.logger.info(f"Created Gold dimension crawler: {crawler_name}")
                        created_crawlers.append(crawler_name)
                    else:
                        raise e
                        
            except ClientError as e:
                self.handle_aws_error(e, f"creating Gold dimension crawler {crawler_name}")
        
        # Fact crawlers
        for fact in self.facts:
            crawler_name = f"meta_gold_{fact}_crawler"
            s3_target = f"s3://{self.s3_bucket_name}/meta/gold/{fact}/"
            
            try:
                try:
                    self.glue_client.get_crawler(Name=crawler_name)
                    self.logger.info(f"Crawler {crawler_name} already exists")
                except ClientError as e:
                    if e.response['Error']['Code'] == 'EntityNotFoundException':
                        self.glue_client.create_crawler(
                            Name=crawler_name,
                            Role=self.glue_role_arn,
                            DatabaseName='meta_gold',
                            Description=f"Crawler cho Gold fact table {fact}",
                            Targets={
                                'S3Targets': [{
                                    'Path': s3_target,
                                    'Exclusions': ['**/_SUCCESS', '**/_temporary/**']
                                }]
                            },
                            TablePrefix=f"{fact}_",
                            SchemaChangePolicy={
                                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                                'DeleteBehavior': 'LOG'
                            },
                            Tags=self._get_crawler_tags(layer='gold', table=fact, resource_type='fact')
                        )
                        self.logger.info(f"Created Gold fact crawler: {crawler_name}")
                        created_crawlers.append(crawler_name)
                    else:
                        raise e
                        
            except ClientError as e:
                self.handle_aws_error(e, f"creating Gold fact crawler {crawler_name}")
        
        return {"created_crawlers": created_crawlers}
    
    def _get_crawler_tags(self, layer: str, table: str, resource_type: str = "table") -> Dict[str, str]:
        """Get tags for crawlers"""
        return self.add_default_tags({
            'Name': f"{layer}-{table}-crawler",
            'Layer': layer,
            'Table': table,
            'ResourceType': resource_type,
            'Service': 'Glue'
        })
    
    def create_glue_jobs_placeholder(self) -> Dict[str, List[str]]:
        """
        Tạo Glue job definitions (placeholder - actual scripts upload riêng)
        Tương tự aws_glue_job resources trong terraform
        """
        job_config = self.config.get('glue_job_settings', {
            'max_concurrent_runs': 3,
            'max_retries': 1,
            'timeout_minutes': 60,
            'worker_type': 'G.1X',
            'number_of_workers': 2
        })
        
        created_jobs = []
        
        # Bronze transformation jobs
        for table in self.tables:
            job_name = f"meta_bronze_{table}_transform"
            script_location = f"s3://{self.s3_bucket_name}/scripts/glue_jobs/bronze/{table}_transform.py"
            
            try:
                try:
                    self.glue_client.get_job(JobName=job_name)
                    self.logger.info(f"Job {job_name} already exists")
                except ClientError as e:
                    if e.response['Error']['Code'] == 'EntityNotFoundException':
                        self.glue_client.create_job(
                            Name=job_name,
                            Role=self.glue_role_arn,
                            Command={
                                'Name': 'glueetl',
                                'ScriptLocation': script_location,
                                'PythonVersion': '3'
                            },
                            DefaultArguments={
                                '--job-bookmark-option': 'job-bookmark-enable',
                                '--enable-metrics': '',
                                '--enable-continuous-cloudwatch-log': 'true',
                                '--table_name': table,
                                '--s3_bucket': self.s3_bucket_name,
                                '--environment': self.environment
                            },
                            MaxRetries=job_config['max_retries'],
                            Timeout=job_config['timeout_minutes'],
                            WorkerType=job_config['worker_type'],
                            NumberOfWorkers=job_config['number_of_workers'],
                            Tags=self.add_default_tags({
                                'Layer': 'bronze',
                                'Table': table,
                                'JobType': 'transformation'
                            })
                        )
                        self.logger.info(f"Created Bronze job: {job_name}")
                        created_jobs.append(job_name)
                    else:
                        raise e
                        
            except ClientError as e:
                self.handle_aws_error(e, f"creating Bronze job {job_name}")
        
        # Silver transformation jobs
        for table in self.tables:
            job_name = f"meta_silver_{table}_transform"
            script_location = f"s3://{self.s3_bucket_name}/scripts/glue_jobs/silver/{table}_transform.py"
            
            try:
                try:
                    self.glue_client.get_job(JobName=job_name)
                    self.logger.info(f"Job {job_name} already exists")
                except ClientError as e:
                    if e.response['Error']['Code'] == 'EntityNotFoundException':
                        self.glue_client.create_job(
                            Name=job_name,
                            Role=self.glue_role_arn,
                            Command={
                                'Name': 'glueetl',
                                'ScriptLocation': script_location,
                                'PythonVersion': '3'
                            },
                            DefaultArguments={
                                '--job-bookmark-option': 'job-bookmark-enable',
                                '--enable-metrics': '',
                                '--enable-continuous-cloudwatch-log': 'true',
                                '--table_name': table,
                                '--s3_bucket': self.s3_bucket_name,
                                '--environment': self.environment
                            },
                            MaxRetries=job_config['max_retries'],
                            Timeout=job_config['timeout_minutes'],
                            WorkerType=job_config['worker_type'],
                            NumberOfWorkers=job_config['number_of_workers'],
                            Tags=self.add_default_tags({
                                'Layer': 'silver',
                                'Table': table,
                                'JobType': 'transformation'
                            })
                        )
                        self.logger.info(f"Created Silver job: {job_name}")
                        created_jobs.append(job_name)
                    else:
                        raise e
                        
            except ClientError as e:
                self.handle_aws_error(e, f"creating Silver job {job_name}")
        
        return {"created_jobs": created_jobs}
    
    def create_athena_workgroup(self) -> Dict[str, str]:
        """
        Tạo Athena workgroup cho analytics
        Tương tự aws_athena_workgroup resource
        """
        try:
            # Check if workgroup exists
            try:
                response = self.athena_client.get_work_group(WorkGroup=self.athena_workgroup_name)
                self.logger.info(f"Athena workgroup {self.athena_workgroup_name} already exists")
                # AWS get_work_group might not return WorkGroupArn, construct it manually
                workgroup_arn = f"arn:aws:athena:{self.aws_region}:{self.account_id}:workgroup/{self.athena_workgroup_name}"
            except ClientError as e:
                if e.response['Error']['Code'] == 'InvalidRequestException':
                    # Create new workgroup
                    response = self.athena_client.create_work_group(
                        Name=self.athena_workgroup_name,
                        Description="Workgroup cho Meta Kaggle Pipeline analytics",
                        Configuration={
                            'ResultConfiguration': {
                                'OutputLocation': self.athena_output_location,
                                'EncryptionConfiguration': {
                                    'EncryptionOption': 'SSE_S3'
                                }
                            },
                            'EnforceWorkGroupConfiguration': True,
                            'PublishCloudWatchMetricsEnabled': True
                        },
                        Tags=[
                            {'Key': k, 'Value': v} 
                            for k, v in self.add_default_tags({
                                'Service': 'Athena',
                                'Purpose': 'analytics'
                            }).items()
                        ]
                    )
                    workgroup_arn = f"arn:aws:athena:{self.aws_region}:*:workgroup/{self.athena_workgroup_name}"
                    self.logger.info(f"Created Athena workgroup: {self.athena_workgroup_name}")
                else:
                    raise e
            
            return {
                "workgroup_name": self.athena_workgroup_name,
                "workgroup_arn": workgroup_arn,
                "output_location": self.athena_output_location
            }
            
        except ClientError as e:
            self.handle_aws_error(e, f"creating Athena workgroup {self.athena_workgroup_name}")
    
    def deploy(self) -> Dict[str, Any]:
        """Deploy complete Glue infrastructure"""
        if not self.glue_role_arn:
            raise ValueError("Glue role ARN must be set before deployment")
        
        self.logger.info("Starting Glue deployment")
        results = {}
        
        try:
            # 1. Create Glue databases
            databases_result = self.create_glue_databases()
            results['databases'] = databases_result
            
            # 2. Create Bronze crawlers
            bronze_crawlers = self.create_bronze_crawlers()
            results['bronze_crawlers'] = bronze_crawlers
            
            # 3. Create Silver crawlers
            silver_crawlers = self.create_silver_crawlers()
            results['silver_crawlers'] = silver_crawlers
            
            # 4. Create Gold crawlers
            gold_crawlers = self.create_gold_crawlers()
            results['gold_crawlers'] = gold_crawlers
            
            # 5. Create Glue jobs (placeholder)
            jobs_result = self.create_glue_jobs_placeholder()
            results['glue_jobs'] = jobs_result
            
            # 6. Create Athena workgroup
            athena_result = self.create_athena_workgroup()
            results['athena'] = athena_result
            
            results['deployment_status'] = 'success'
            
            self.logger.info("Glue deployment completed successfully")
            return results
            
        except Exception as e:
            self.logger.error(f"Glue deployment failed: {e}")
            results['deployment_status'] = 'failed'
            results['error'] = str(e)
            return results
    
    def get_deployment_info(self) -> Dict[str, Any]:
        """Get deployment information"""
        return {
            'service': 'Glue',
            'databases': {
                'bronze': 'meta_bronze',
                'silver': 'meta_silver', 
                'gold': 'meta_gold'
            },
            'crawler_counts': {
                'bronze': len(self.tables),
                'silver': len(self.tables),
                'gold': len(self.dimensions) + len(self.facts)
            },
            'job_counts': {
                'bronze': len(self.tables),
                'silver': len(self.tables),
                'gold': len(self.dimensions) + len(self.facts)
            },
            'athena_workgroup': self.athena_workgroup_name,
            'athena_output_location': self.athena_output_location,
            'glue_role_arn': self.glue_role_arn,
            'region': self.aws_region,
            'environment': self.environment
        }
    
    def resource_exists(self, resource_identifier: str) -> bool:
        """Check if Glue resource exists"""
        # Implementation would depend on specific resource type
        return False