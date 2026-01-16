"""
AWS S3 Data Loader Module
Provides functionality to load data from AWS S3 buckets.
"""

import logging
import os
from pathlib import Path
from typing import Optional, Dict, Any, List
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class S3DataLoader:
    """
    Handles loading data from AWS S3 buckets.
    """
    
    def __init__(self, aws_access_key_id: Optional[str] = None, 
                 aws_secret_access_key: Optional[str] = None,
                 region_name: str = 'us-east-1'):
        """
        Initialize S3 Data Loader.
        
        Args:
            aws_access_key_id: AWS Access Key ID (if None, uses environment variable)
            aws_secret_access_key: AWS Secret Access Key (if None, uses environment variable)
            region_name: AWS region name
        """
        self.aws_access_key_id = aws_access_key_id or os.environ.get('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = aws_secret_access_key or os.environ.get('AWS_SECRET_ACCESS_KEY')
        self.region_name = region_name
        
        # Import boto3 only when needed
        try:
            import boto3
            self.boto3 = boto3
            self._s3_client = None
            logger.info(f"S3DataLoader initialized for region: {region_name}")
        except ImportError:
            logger.warning("boto3 not installed. S3 functionality will not be available.")
            self.boto3 = None
    
    @property
    def s3_client(self):
        """Lazy initialization of S3 client."""
        if self._s3_client is None and self.boto3:
            self._s3_client = self.boto3.client(
                's3',
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region_name
            )
            logger.info("S3 client created successfully")
        return self._s3_client
    
    def list_objects(self, bucket: str, prefix: str = '') -> List[str]:
        """
        List objects in an S3 bucket with a given prefix.
        
        Args:
            bucket: S3 bucket name
            prefix: Prefix to filter objects
            
        Returns:
            List of object keys
        """
        if not self.boto3:
            raise ImportError("boto3 is required for S3 operations. Install with: pip install boto3")
        
        try:
            logger.info(f"Listing objects in bucket: {bucket}, prefix: {prefix}")
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            
            if 'Contents' not in response:
                logger.warning(f"No objects found in {bucket}/{prefix}")
                return []
            
            objects = [obj['Key'] for obj in response['Contents']]
            logger.info(f"Found {len(objects)} objects")
            return objects
        
        except Exception as e:
            logger.error(f"Error listing objects: {str(e)}")
            raise
    
    def download_file(self, bucket: str, key: str, local_path: str) -> str:
        """
        Download a file from S3 to local path.
        
        Args:
            bucket: S3 bucket name
            key: S3 object key
            local_path: Local file path to save the downloaded file
            
        Returns:
            Local file path
        """
        if not self.boto3:
            raise ImportError("boto3 is required for S3 operations. Install with: pip install boto3")
        
        try:
            logger.info(f"Downloading s3://{bucket}/{key} to {local_path}")
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            self.s3_client.download_file(bucket, key, local_path)
            logger.info(f"File downloaded successfully to {local_path}")
            return local_path
        
        except Exception as e:
            logger.error(f"Error downloading file: {str(e)}")
            raise
    
    def load_csv_from_s3(self, bucket: str, key: str, **kwargs) -> pd.DataFrame:
        """
        Load CSV file directly from S3 into a pandas DataFrame.
        
        Args:
            bucket: S3 bucket name
            key: S3 object key
            **kwargs: Additional arguments to pass to pd.read_csv
            
        Returns:
            pandas DataFrame
        """
        if not self.boto3:
            raise ImportError("boto3 is required for S3 operations. Install with: pip install boto3")
        
        try:
            logger.info(f"Loading CSV from s3://{bucket}/{key}")
            
            # Use s3fs for direct pandas integration
            try:
                import s3fs
                s3_path = f"s3://{bucket}/{key}"
                
                fs = s3fs.S3FileSystem(
                    key=self.aws_access_key_id,
                    secret=self.aws_secret_access_key,
                    client_kwargs={'region_name': self.region_name}
                )
                
                with fs.open(s3_path, 'rb') as f:
                    df = pd.read_csv(f, **kwargs)
                
                logger.info(f"Loaded CSV with shape: {df.shape}")
                return df
            
            except ImportError:
                logger.warning("s3fs not installed. Falling back to download method.")
                # Fallback: download file first, then load
                import tempfile
                with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
                    tmp_path = tmp.name
                
                self.download_file(bucket, key, tmp_path)
                df = pd.read_csv(tmp_path, **kwargs)
                
                # Cleanup
                os.remove(tmp_path)
                
                logger.info(f"Loaded CSV with shape: {df.shape}")
                return df
        
        except Exception as e:
            logger.error(f"Error loading CSV from S3: {str(e)}")
            raise
    
    def load_json_from_s3(self, bucket: str, key: str, **kwargs) -> Any:
        """
        Load JSON file from S3.
        
        Args:
            bucket: S3 bucket name
            key: S3 object key
            **kwargs: Additional arguments to pass to pd.read_json
            
        Returns:
            Loaded JSON data
        """
        if not self.boto3:
            raise ImportError("boto3 is required for S3 operations. Install with: pip install boto3")
        
        try:
            logger.info(f"Loading JSON from s3://{bucket}/{key}")
            
            import tempfile
            with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as tmp:
                tmp_path = tmp.name
            
            self.download_file(bucket, key, tmp_path)
            
            import json
            with open(tmp_path, 'r') as f:
                data = json.load(f)
            
            # Cleanup
            os.remove(tmp_path)
            
            logger.info(f"Loaded JSON successfully")
            return data
        
        except Exception as e:
            logger.error(f"Error loading JSON from S3: {str(e)}")
            raise
    
    def upload_file(self, local_path: str, bucket: str, key: str) -> bool:
        """
        Upload a local file to S3.
        
        Args:
            local_path: Local file path
            bucket: S3 bucket name
            key: S3 object key
            
        Returns:
            True if successful
        """
        if not self.boto3:
            raise ImportError("boto3 is required for S3 operations. Install with: pip install boto3")
        
        try:
            logger.info(f"Uploading {local_path} to s3://{bucket}/{key}")
            self.s3_client.upload_file(local_path, bucket, key)
            logger.info("File uploaded successfully")
            return True
        
        except Exception as e:
            logger.error(f"Error uploading file: {str(e)}")
            raise
    
    def upload_dataframe(self, df: pd.DataFrame, bucket: str, key: str, 
                        format: str = 'csv', **kwargs) -> bool:
        """
        Upload a pandas DataFrame to S3.
        
        Args:
            df: pandas DataFrame
            bucket: S3 bucket name
            key: S3 object key
            format: File format ('csv' or 'json')
            **kwargs: Additional arguments for to_csv or to_json
            
        Returns:
            True if successful
        """
        if not self.boto3:
            raise ImportError("boto3 is required for S3 operations. Install with: pip install boto3")
        
        try:
            import tempfile
            
            logger.info(f"Uploading DataFrame to s3://{bucket}/{key} as {format}")
            
            with tempfile.NamedTemporaryFile(suffix=f'.{format}', delete=False, mode='w') as tmp:
                tmp_path = tmp.name
                
                if format == 'csv':
                    df.to_csv(tmp_path, index=False, **kwargs)
                elif format == 'json':
                    df.to_json(tmp_path, **kwargs)
                else:
                    raise ValueError(f"Unsupported format: {format}")
            
            self.upload_file(tmp_path, bucket, key)
            
            # Cleanup
            os.remove(tmp_path)
            
            logger.info("DataFrame uploaded successfully")
            return True
        
        except Exception as e:
            logger.error(f"Error uploading DataFrame: {str(e)}")
            raise


def main():
    """
    Example usage of S3DataLoader.
    """
    logger.info("=" * 80)
    logger.info("S3 Data Loader Example")
    logger.info("=" * 80)
    
    # Example: Initialize loader
    try:
        loader = S3DataLoader()
        logger.info("S3DataLoader initialized successfully")
        
        # Note: Actual S3 operations require valid credentials
        logger.info("To use S3 operations, set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
        
    except Exception as e:
        logger.error(f"Failed to initialize S3DataLoader: {str(e)}")
    
    logger.info("=" * 80)


if __name__ == '__main__':
    main()
