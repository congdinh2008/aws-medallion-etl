#!/usr/bin/env python3
"""
S3Manager cho Meta Kaggle Pipeline
Tạo S3 Data Lake theo Medallion Architecture (Bronze/Silver/Gold)
Tương tự terraform/modules/s3/main.tf
"""

import json
from typing import Dict, List, Any, Optional
from botocore.exceptions import ClientError

from .base_manager import BaseInfrastructureManager


class S3Manager(BaseInfrastructureManager):
    """
    Manager cho S3 Data Lake với Medallion Architecture
    Tương tự terraform/modules/s3/
    """
    
    def __init__(self, config: Dict[str, Any], session=None):
        """Khởi tạo S3Manager"""
        super().__init__(config, session)
        self.s3_client = self.get_client('s3')
        self.bucket_name = config['s3_bucket_name']
        
        # Tables và layers từ config
        self.tables = config.get('medallion_tables', [
            'users', 'datasets', 'competitions', 'tags', 'kernels'
        ])
        self.layers = config.get('medallion_layers', [
            'raw', 'bronze', 'silver', 'gold'
        ])
    
    def validate_config(self) -> bool:
        """Validate S3 configuration"""
        if not self.bucket_name:
            raise ValueError("S3 bucket name is required")
        
        # Validate bucket name format
        if not self._is_valid_bucket_name(self.bucket_name):
            raise ValueError(f"Invalid S3 bucket name: {self.bucket_name}")
        
        return True
    
    def _is_valid_bucket_name(self, name: str) -> bool:
        """Check bucket name theo AWS S3 requirements"""
        if len(name) < 3 or len(name) > 63:
            return False
        if name.startswith('-') or name.endswith('-'):
            return False
        if '..' in name:
            return False
        return True
    
    def bucket_exists(self) -> bool:
        """Check xem bucket có tồn tại không"""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                return False
            else:
                self.handle_aws_error(e, f"checking bucket {self.bucket_name}")
                return False
    
    def create_bucket(self) -> Dict[str, Any]:
        """
        Tạo S3 bucket chính với configuration
        Tương tự aws_s3_bucket resource trong terraform
        """
        try:
            if self.bucket_exists():
                self.logger.info(f"Bucket {self.bucket_name} already exists")
                return {"bucket_name": self.bucket_name, "created": False}
            
            # Tạo bucket
            if self.aws_region == 'us-east-1':
                # us-east-1 không cần LocationConstraint
                self.s3_client.create_bucket(Bucket=self.bucket_name)
            else:
                self.s3_client.create_bucket(
                    Bucket=self.bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': self.aws_region}
                )
            
            self.logger.info(f"Created bucket: {self.bucket_name}")
            
            # Add tags
            self._tag_bucket()
            
            return {"bucket_name": self.bucket_name, "created": True}
            
        except ClientError as e:
            self.handle_aws_error(e, f"creating bucket {self.bucket_name}")
    
    def _tag_bucket(self) -> None:
        """Add tags to bucket"""
        tags = self.add_default_tags({
            'Name': 'Meta Data Lake',
            'Layer': 'Storage',
            'Architecture': 'Medallion',
            'DataClass': 'processed'
        })
        
        tag_set = [{'Key': k, 'Value': v} for k, v in tags.items()]
        
        try:
            self.s3_client.put_bucket_tagging(
                Bucket=self.bucket_name,
                Tagging={'TagSet': tag_set}
            )
            self.logger.info(f"Added tags to bucket {self.bucket_name}")
        except ClientError as e:
            self.handle_aws_error(e, f"tagging bucket {self.bucket_name}")
    
    def enable_versioning(self) -> None:
        """
        Enable versioning cho bucket
        Tương tự aws_s3_bucket_versioning trong terraform
        """
        try:
            self.s3_client.put_bucket_versioning(
                Bucket=self.bucket_name,
                VersioningConfiguration={'Status': 'Enabled'}
            )
            self.logger.info(f"Enabled versioning for bucket {self.bucket_name}")
        except ClientError as e:
            self.handle_aws_error(e, f"enabling versioning for {self.bucket_name}")
    
    def configure_encryption(self) -> None:
        """
        Configure server-side encryption
        Tương tự aws_s3_bucket_server_side_encryption_configuration
        """
        encryption_config = {
            'Rules': [{
                'ApplyServerSideEncryptionByDefault': {
                    'SSEAlgorithm': 'AES256'
                }
            }]
        }
        
        try:
            self.s3_client.put_bucket_encryption(
                Bucket=self.bucket_name,
                ServerSideEncryptionConfiguration=encryption_config
            )
            self.logger.info(f"Configured encryption for bucket {self.bucket_name}")
        except ClientError as e:
            self.handle_aws_error(e, f"configuring encryption for {self.bucket_name}")
    
    def block_public_access(self) -> None:
        """
        Block all public access
        Tương tự aws_s3_bucket_public_access_block
        """
        try:
            self.s3_client.put_public_access_block(
                Bucket=self.bucket_name,
                PublicAccessBlockConfiguration={
                    'BlockPublicAcls': True,
                    'IgnorePublicAcls': True,
                    'BlockPublicPolicy': True,
                    'RestrictPublicBuckets': True
                }
            )
            self.logger.info(f"Configured public access block for {self.bucket_name}")
        except ClientError as e:
            self.handle_aws_error(e, f"configuring public access block for {self.bucket_name}")
    
    def create_folder_structure(self) -> Dict[str, List[str]]:
        """
        Tạo folder structure cho Medallion Architecture
        Tương tự aws_s3_object resources trong terraform
        """
        created_folders = []
        
        # Tạo folders cho từng combination của layer và table
        for layer in self.layers:
            for table in self.tables:
                folder_key = f"meta/{layer}/{table}/"
                try:
                    self.s3_client.put_object(
                        Bucket=self.bucket_name,
                        Key=folder_key,
                        Body='',  # Empty content for folder
                        ContentType='application/x-directory'
                    )
                    created_folders.append(folder_key)
                    self.logger.debug(f"Created folder: {folder_key}")
                except ClientError as e:
                    self.logger.error(f"Failed to create folder {folder_key}: {e}")
        
        # Tạo special folders
        special_folders = [
            "meta/bronze/_rejects/",
            "meta/bronze/_reports/", 
            "meta/silver/_reports/",
            "meta/gold/_reports/",
            "meta/gold/_logs/",
            "meta/athena-results/",
            "scripts/glue_jobs/",
            "scripts/sql/",
            "configs/",
            "temp/"
        ]
        
        for folder_key in special_folders:
            try:
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=folder_key,
                    Body='',
                    ContentType='application/x-directory'
                )
                created_folders.append(folder_key)
                self.logger.debug(f"Created special folder: {folder_key}")
            except ClientError as e:
                self.logger.error(f"Failed to create special folder {folder_key}: {e}")
        
        self.logger.info(f"Created {len(created_folders)} folders")
        return {"folders_created": created_folders}
    
    def configure_lifecycle_rules(self) -> None:
        """
        Configure lifecycle rules for cost optimization
        Tương tự aws_s3_bucket_lifecycle_configuration
        """
        if not self.config.get('enable_cost_optimization', True):
            self.logger.info("Cost optimization disabled, skipping lifecycle rules")
            return
        
        lifecycle_rules = [
            # Raw data lifecycle - keep longer as source of truth
            {
                'ID': 'raw_data_lifecycle',
                'Status': 'Enabled',
                'Filter': {'Prefix': 'meta/raw/'},
                'Transitions': [
                    {
                        'Days': 30,
                        'StorageClass': 'STANDARD_IA'
                    },
                    {
                        'Days': 90, 
                        'StorageClass': 'GLACIER'
                    },
                    {
                        'Days': 365,
                        'StorageClass': 'DEEP_ARCHIVE'
                    }
                ]
            },
            # Bronze layer lifecycle
            {
                'ID': 'bronze_data_lifecycle',
                'Status': 'Enabled',
                'Filter': {'Prefix': 'meta/bronze/'},
                'Transitions': [
                    {
                        'Days': 60,
                        'StorageClass': 'STANDARD_IA'
                    },
                    {
                        'Days': 180,
                        'StorageClass': 'GLACIER'
                    }
                ]
            },
            # Silver layer lifecycle
            {
                'ID': 'silver_data_lifecycle',
                'Status': 'Enabled',
                'Filter': {'Prefix': 'meta/silver/'},
                'Transitions': [
                    {
                        'Days': 90,
                        'StorageClass': 'STANDARD_IA'  
                    },
                    {
                        'Days': 365,
                        'StorageClass': 'GLACIER'
                    }
                ]
            },
            # Cleanup incomplete multipart uploads
            {
                'ID': 'incomplete_multipart_cleanup',
                'Status': 'Enabled',
                'Filter': {},  # Empty filter applies to entire bucket
                'AbortIncompleteMultipartUpload': {
                    'DaysAfterInitiation': 7
                }
            },
            # Cleanup old rejected records
            {
                'ID': 'rejects_cleanup',
                'Status': 'Enabled',
                'Filter': {'Prefix': 'meta/bronze/_rejects/'},
                'Expiration': {
                    'Days': 90  # Delete rejects after 90 days
                }
            },
            # Athena results cleanup
            {
                'ID': 'athena_results_cleanup',
                'Status': 'Enabled',
                'Filter': {'Prefix': 'meta/athena-results/'},
                'Expiration': {
                    'Days': 30  # Delete Athena results after 30 days
                }
            }
        ]
        
        try:
            self.s3_client.put_bucket_lifecycle_configuration(
                Bucket=self.bucket_name,
                LifecycleConfiguration={'Rules': lifecycle_rules}
            )
            self.logger.info(f"Configured lifecycle rules for {self.bucket_name}")
        except ClientError as e:
            self.handle_aws_error(e, f"configuring lifecycle rules for {self.bucket_name}")
    
    def configure_intelligent_tiering(self) -> None:
        """
        Configure intelligent tiering for cost optimization
        Tương tự aws_s3_bucket_intelligent_tiering_configuration
        """
        if not self.config.get('enable_cost_optimization', True):
            self.logger.info("Cost optimization disabled, skipping intelligent tiering")
            return
        
        try:
            self.s3_client.put_bucket_intelligent_tiering_configuration(
                Bucket=self.bucket_name,
                Id='meta-intelligent-tiering',
                IntelligentTieringConfiguration={
                    'Id': 'meta-intelligent-tiering',
                    'Status': 'Enabled',
                    'Filter': {'Prefix': 'meta/'},
                    'Tierings': [
                        {
                            'Days': 90,  # AWS minimum is 90 days for ARCHIVE_ACCESS
                            'AccessTier': 'ARCHIVE_ACCESS'
                        },
                        {
                            'Days': 180,  # AWS minimum is 180 days for DEEP_ARCHIVE_ACCESS
                            'AccessTier': 'DEEP_ARCHIVE_ACCESS'
                        }
                    ]
                }
            )
            self.logger.info(f"Configured intelligent tiering for {self.bucket_name}")
        except ClientError as e:
            # Intelligent tiering might not be available in all regions
            self.logger.warning(f"Could not configure intelligent tiering: {e}")
    
    def deploy(self) -> Dict[str, Any]:
        """
        Deploy complete S3 infrastructure
        Main entry point tương tự terraform module
        """
        self.logger.info("Starting S3 Data Lake deployment")
        results = {}
        
        try:
            # 1. Create bucket
            bucket_result = self.create_bucket()
            results.update(bucket_result)
            
            # 2. Configure bucket settings
            if bucket_result.get('created', False) or not self.bucket_exists():
                self.enable_versioning()
                self.configure_encryption()
                self.block_public_access()
            
            # 3. Create folder structure
            folder_result = self.create_folder_structure()
            results.update(folder_result)
            
            # 4. Configure cost optimization
            self.configure_lifecycle_rules()
            self.configure_intelligent_tiering()
            
            results.update({
                'deployment_status': 'success',
                'bucket_arn': f"arn:aws:s3:::{self.bucket_name}",
                'bucket_domain_name': f"{self.bucket_name}.s3.amazonaws.com",
                'bucket_regional_domain_name': f"{self.bucket_name}.s3.{self.aws_region}.amazonaws.com"
            })
            
            self.logger.info("S3 Data Lake deployment completed successfully")
            return results
            
        except Exception as e:
            self.logger.error(f"S3 deployment failed: {e}")
            results['deployment_status'] = 'failed'
            results['error'] = str(e)
            return results
    
    def get_deployment_info(self) -> Dict[str, Any]:
        """Lấy thông tin deployment"""
        return {
            'service': 'S3',
            'bucket_name': self.bucket_name,
            'bucket_arn': f"arn:aws:s3:::{self.bucket_name}",
            'region': self.aws_region,
            'medallion_layers': self.layers,
            'tables': self.tables,
            'data_paths': {
                'raw': f"s3://{self.bucket_name}/meta/raw/",
                'bronze': f"s3://{self.bucket_name}/meta/bronze/",
                'silver': f"s3://{self.bucket_name}/meta/silver/",
                'gold': f"s3://{self.bucket_name}/meta/gold/",
                'athena_results': f"s3://{self.bucket_name}/meta/athena-results/",
                'glue_scripts': f"s3://{self.bucket_name}/scripts/glue_jobs/"
            }
        }
    
    def resource_exists(self, resource_identifier: str) -> bool:
        """Check xem S3 bucket có tồn tại không"""
        return self.bucket_exists()
    
    def delete_bucket(self, force: bool = False) -> Dict[str, Any]:
        """
        Delete bucket và tất cả contents (BE CAREFUL!)
        
        Args:
            force: Có force delete tất cả objects trước
        """
        if not self.bucket_exists():
            return {"status": "bucket_not_found"}
        
        try:
            if force:
                # Delete all objects first
                self._empty_bucket()
            
            self.s3_client.delete_bucket(Bucket=self.bucket_name)
            self.logger.info(f"Deleted bucket: {self.bucket_name}")
            return {"status": "deleted"}
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'BucketNotEmpty':
                return {"status": "bucket_not_empty", "error": "Bucket contains objects"}
            else:
                self.handle_aws_error(e, f"deleting bucket {self.bucket_name}")
    
    def _empty_bucket(self) -> None:
        """Empty bucket by deleting all objects"""
        try:
            # List và delete objects
            paginator = self.s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=self.bucket_name):
                if 'Contents' in page:
                    objects = [{'Key': obj['Key']} for obj in page['Contents']]
                    if objects:
                        self.s3_client.delete_objects(
                            Bucket=self.bucket_name,
                            Delete={'Objects': objects}
                        )
            
            # Delete object versions if versioning is enabled
            paginator = self.s3_client.get_paginator('list_object_versions')
            for page in paginator.paginate(Bucket=self.bucket_name):
                versions = []
                if 'Versions' in page:
                    versions.extend([
                        {'Key': v['Key'], 'VersionId': v['VersionId']}
                        for v in page['Versions']
                    ])
                if 'DeleteMarkers' in page:
                    versions.extend([
                        {'Key': d['Key'], 'VersionId': d['VersionId']}
                        for d in page['DeleteMarkers']
                    ])
                
                if versions:
                    self.s3_client.delete_objects(
                        Bucket=self.bucket_name,
                        Delete={'Objects': versions}
                    )
            
            self.logger.info(f"Emptied bucket: {self.bucket_name}")
            
        except ClientError as e:
            self.handle_aws_error(e, f"emptying bucket {self.bucket_name}")