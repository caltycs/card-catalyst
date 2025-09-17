"""
Data Reader module for reading transaction data from S3
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from utilities.config import Config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataReader:
    """Handles reading data from S3"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.schema = self._get_transaction_schema()
    
    def _get_transaction_schema(self) -> StructType:
        """Define the schema for transaction data"""
        return StructType([
            StructField("transaction_id", StringType(), False),
            StructField("merchant_id", StringType(), False),
            StructField("card_number", StringType(), False),
            StructField("amount", DoubleType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("customer_id", StringType(), False),
            StructField("currency", StringType(), False),
            StructField("location", StringType(), False)
        ])

    def read_transactions_from_s3(self, s3_path: str = None) -> DataFrame:
        """
        Read transaction data from S3
        
        Args:
            s3_path: Optional specific S3 path, defaults to config path
            
        Returns:
            DataFrame containing transaction data
        """
        try:
            if s3_path is None:
                s3_path = Config.S3_RAW_PATH
            logger.info(f"Reading transaction data from: {s3_path}")
            # Read JSON files with the defined schema
            df = (self.spark
                  .read
                  .schema(self.schema)
                  .option("multiline", "true")
                  .option("mode", "PERMISSIVE")
                  .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss'Z'")
                  .json(s3_path))

            logger.info(f"Successfully read {df.count()} records from S3")
            return df
        except Exception as e:
            logger.error(f"Error reading data from S3: {str(e)}")
            raise

    def read_transactions_from_local(self, local_path)-> DataFrame:
        """
            Read transaction data from Local File System

            Args:
                local_path: Specific Local path, defaults to config path

            Returns:
                DataFrame containing transaction data
        """
        try:
            if local_path is None:
                local_path = Config.LOCAL_RAW_PATH
            logger.info(f"Reading transaction data from: {local_path}")
            # Read JSON files with the defined schema
            df = (self.spark
                  .read
                  .schema(self.schema)
                  .option("multiline", "true")
                  .option("mode", "PERMISSIVE")
                  .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss'Z'")
                  .json(local_path))

            logger.info(f"Successfully read {df.count()} records from Local")
            return df
        except Exception as e:
            logger.error(f"Error reading data from Local: {str(e)}")
            raise

    def read_transactions_by_date(self, year: str, month: str, day: str, mode:str) -> DataFrame:
        """
        Read transaction data for a specific date
        
        Args:
            year: Year (e.g., "2025")
            month: Month (e.g., "06")
            day: Day (e.g., "24")
            
        Returns:
            DataFrame containing transaction data for the specified date
        """
        if mode == "s3":
            data_path = f"{Config.S3_RAW_PATH}year={year}/month={month}/day={day}/"
            return self.read_transactions_from_s3(data_path)
        elif mode =="local":
            data_path= f"{Config.LOCAL_RAW_PATH}/{year}_{month}_{day}.json"
            return self.read_transactions_from_local(data_path)
        
    
    def read_transactions_date_range(self, start_date: str, end_date: str) -> DataFrame:
        """
        Read transaction data for a date range
        Note: This is a simplified implementation. In production, you might want
        to use more sophisticated date range filtering
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            DataFrame containing transaction data for the date range
        """
        # For simplicity, reading all data and filtering
        # In production, you might want to optimize this by reading specific partitions
        df = self.read_transactions_from_s3()
        
        return (df
                .filter(df.timestamp.between(start_date, end_date))
                .cache())
    
    def validate_data_availability(self, s3_path: str = None) -> bool:
        """
        Check if data is available at the specified S3 path
        
        Args:
            s3_path: S3 path to validate
            
        Returns:
            Boolean indicating if data is available
        """
        try:
            if s3_path is None:
                s3_path = Config.S3_RAW_PATH
                
            # Try to read the first row to validate availability
            df = (self.spark
                  .read
                  .option("multiline", "true")
                  .json(s3_path)
                  .limit(1))
            
            return df.count() > 0
            
        except Exception as e:
            logger.warning(f"Data not available at {s3_path}: {str(e)}")
            return False

