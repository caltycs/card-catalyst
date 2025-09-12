"""
Spark Session Manager for transaction processing pipeline
"""

from pyspark.sql import SparkSession
from .config import Config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SparkSessionManager:
    """Manages Spark session creation and configuration"""
    
    _instance = None
    _spark = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SparkSessionManager, cls).__new__(cls)
        return cls._instance
    
    def get_spark_session(self) -> SparkSession:
        """Create or return existing Spark session"""
        if self._spark is None:
            logger.info("Creating new Spark session...")
            
            builder = SparkSession.builder.appName(Config.SPARK_CONFIG['app_name'])
            
            # Set master if specified
            if Config.SPARK_CONFIG.get('master'):
                builder = builder.master(Config.SPARK_CONFIG['master'])
            
            # Add configurations
            for key, value in Config.SPARK_CONFIG['config'].items():
                builder = builder.config(key, value)
            
            self._spark = builder.getOrCreate()
            self._spark.sparkContext.setLogLevel("WARN")
            
            logger.info(f"Spark session created successfully with version: {self._spark.version}")
        
        return self._spark
    
    def stop_spark_session(self):
        """Stop the Spark session"""
        if self._spark is not None:
            logger.info("Stopping Spark session...")
            self._spark.stop()
            self._spark = None
            logger.info("Spark session stopped successfully")

# Global instance
spark_manager = SparkSessionManager()