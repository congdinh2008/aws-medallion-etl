#!/usr/bin/env python3
"""
Base Infrastructure Manager cho Meta Kaggle Pipeline
Cung cấp common functionality cho tất cả managers
"""

import boto3
import json
import logging
import time
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from botocore.exceptions import ClientError, BotoCoreError


class BaseInfrastructureManager(ABC):
    """Base class cho tất cả infrastructure managers"""
    
    def __init__(self, config: Dict[str, Any], session: Optional[boto3.Session] = None):
        """
        Khởi tạo base manager
        
        Args:
            config: Configuration dictionary từ config file
            session: Boto3 session (optional, tạo mới nếu None)
        """
        self.config = config
        self.environment = config.get('environment', 'dev')
        self.aws_region = config.get('aws_region', 'us-west-1')
        self.project_name = config.get('project_name', 'meta-kaggle-pipeline')
        self.tags = config.get('common_tags', {})
        
        # Thiết lập session và clients
        self.session = session or boto3.Session(region_name=self.aws_region)
        self.logger = self._setup_logger()
        
        # Clients sẽ được khởi tạo trong subclasses
        self.clients = {}
        
    def _setup_logger(self) -> logging.Logger:
        """Thiết lập logger cho manager"""
        logger_name = f"meta_pipeline.{self.__class__.__name__}"
        logger = logging.getLogger(logger_name)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
            
        return logger
    
    def get_client(self, service_name: str):
        """Lấy hoặc tạo boto3 client cho service"""
        if service_name not in self.clients:
            self.clients[service_name] = self.session.client(service_name)
        return self.clients[service_name]
    
    def add_default_tags(self, tags: Dict[str, str] = None) -> Dict[str, str]:
        """Thêm default tags vào custom tags"""
        default_tags = {
            'Environment': self.environment,
            'Project': 'meta-kaggle-pipeline',
            'ManagedBy': 'boto3-deployment',
            'Region': self.aws_region
        }
        
        # Merge với common tags từ config
        default_tags.update(self.tags)
        
        # Merge với tags được truyền vào
        if tags:
            default_tags.update(tags)
            
        return default_tags
    
    def wait_for_resource(self, check_function, max_attempts: int = 30, 
                         delay_seconds: int = 10) -> bool:
        """
        Chờ resource được tạo/ready
        
        Args:
            check_function: Function để check resource status
            max_attempts: Số lần thử tối đa
            delay_seconds: Delay giữa các lần thử
            
        Returns:
            True nếu resource ready, False nếu timeout
        """
        attempts = 0
        while attempts < max_attempts:
            try:
                if check_function():
                    return True
            except Exception as e:
                self.logger.debug(f"Check function failed (attempt {attempts + 1}): {e}")
            
            time.sleep(delay_seconds)
            attempts += 1
            
        return False
    
    def handle_aws_error(self, error: Exception, operation: str) -> None:
        """
        Xử lý AWS errors một cách thống nhất
        
        Args:
            error: Exception được raise
            operation: Tên operation đang thực hiện
        """
        if isinstance(error, ClientError):
            error_code = error.response.get('Error', {}).get('Code', 'Unknown')
            error_message = error.response.get('Error', {}).get('Message', str(error))
            self.logger.error(f"AWS Error in {operation}: {error_code} - {error_message}")
        else:
            self.logger.error(f"Error in {operation}: {str(error)}")
        raise error
    
    def resource_exists(self, resource_identifier: str) -> bool:
        """
        Check xem resource có tồn tại không
        Subclasses override method này cho từng resource type
        """
        return False
    
    def format_resource_name(self, base_name: str, resource_type: str = "") -> str:
        """
        Format resource name theo convention
        
        Args:
            base_name: Tên cơ bản
            resource_type: Loại resource (optional)
            
        Returns:
            Formatted resource name
        """
        prefix = f"meta-{self.environment}"
        if resource_type:
            return f"{prefix}-{base_name}-{resource_type}"
        else:
            return f"{prefix}-{base_name}"
    
    @abstractmethod
    def deploy(self) -> Dict[str, Any]:
        """
        Deploy resources - phải được implement trong subclasses
        
        Returns:
            Dictionary chứa thông tin về resources được deploy
        """
        pass
    
    @abstractmethod
    def validate_config(self) -> bool:
        """
        Validate configuration - phải được implement trong subclasses
        
        Returns:
            True nếu config valid, raise exception nếu invalid
        """
        pass
    
    @abstractmethod
    def get_deployment_info(self) -> Dict[str, Any]:
        """
        Lấy thông tin về deployment
        
        Returns:
            Dictionary chứa thông tin deployment
        """
        pass
    
    def cleanup(self) -> None:
        """Cleanup resources nếu cần"""
        # Close clients
        for client in self.clients.values():
            if hasattr(client, 'close'):
                client.close()
        self.logger.info(f"{self.__class__.__name__} cleanup completed")


class AWSResourceWaiter:
    """Helper class để wait cho AWS resources"""
    
    def __init__(self, client, logger: logging.Logger):
        self.client = client
        self.logger = logger
    
    def wait_for_stack_complete(self, stack_name: str, max_attempts: int = 60) -> bool:
        """Wait cho CloudFormation stack complete"""
        # Implementation tùy thuộc vào việc có sử dụng CloudFormation không
        pass
    
    def wait_for_resource_ready(self, resource_type: str, resource_id: str,
                              check_function, max_attempts: int = 30) -> bool:
        """Generic waiter cho bất kỳ resource nào"""
        attempts = 0
        while attempts < max_attempts:
            try:
                if check_function(resource_id):
                    self.logger.info(f"{resource_type} {resource_id} is ready")
                    return True
            except Exception as e:
                self.logger.debug(f"Waiting for {resource_type} {resource_id}: {e}")
            
            time.sleep(10)
            attempts += 1
            
        self.logger.error(f"Timeout waiting for {resource_type} {resource_id}")
        return False