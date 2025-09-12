"""
Data Writer module for storing processed data to S3 and MySQL
"""

from pyspark.sql import DataFrame
from src.utilities.config import Config, get_mysql_jdbc_url, get_mysql_properties
import logging
from typing import Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataWriter:
    """Handles writing data to various storage systems"""
    
    def __init__(self):
        self.s3_processed_path = Config.S3_PROCESSED_PATH
        self.s3_summary_path = Config.S3_SUMMARY_PATH
        self.mysql_jdbc_url = get_mysql_jdbc_url()
        self.mysql_properties = get_mysql_properties()
    
    def write_to_s3_parquet(self, df: DataFrame, s3_path: str, partition_cols: list = None, mode: str = 'overwrite') -> None:
        """
        Write DataFrame to S3 in Parquet format
        
        Args:
            df: DataFrame to write
            s3_path: S3 destination path
            partition_cols: Columns to partition by
            mode: Write mode ('overwrite', 'append', etc.)
        """
        try:
            logger.info(f"Writing data to S3: {s3_path}")
            
            writer = df.coalesce(4).write.mode(mode)  # Reduce number of files
            
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            
            writer.parquet(s3_path)
            
            record_count = df.count()
            logger.info(f"Successfully wrote {record_count} records to S3: {s3_path}")
            
        except Exception as e:
            logger.error(f"Error writing to S3 {s3_path}: {str(e)}")
            raise
    
    def write_to_s3_json(self, df: DataFrame, s3_path: str, mode: str = 'overwrite') -> None:
        """
        Write DataFrame to S3 in JSON format
        
        Args:
            df: DataFrame to write
            s3_path: S3 destination path
            mode: Write mode
        """
        try:
            logger.info(f"Writing JSON data to S3: {s3_path}")
            
            df.coalesce(1).write.mode(mode).json(s3_path)
            
            record_count = df.count()
            logger.info(f"Successfully wrote {record_count} records to S3 as JSON: {s3_path}")
            
        except Exception as e:
            logger.error(f"Error writing JSON to S3 {s3_path}: {str(e)}")
            raise
    
    def write_to_mysql(self, df: DataFrame, table_name: str, mode: str = 'overwrite') -> None:
        """
        Write DataFrame to MySQL database
        
        Args:
            df: DataFrame to write
            table_name: MySQL table name
            mode: Write mode ('overwrite', 'append', etc.)
        """
        try:
            logger.info(f"Writing data to MySQL table: {table_name}")
            
            # Prepare properties for MySQL connection
            properties = self.mysql_properties.copy()
            properties['batchsize'] = '10000'  # Optimize batch size
            properties['truncate'] = 'true' if mode == 'overwrite' else 'false'
            
            df.write \
              .jdbc(url=self.mysql_jdbc_url,
                    table=table_name,
                    mode=mode,
                    properties=properties)
            
            record_count = df.count()
            logger.info(f"Successfully wrote {record_count} records to MySQL table: {table_name}")
            
        except Exception as e:
            logger.error(f"Error writing to MySQL table {table_name}: {str(e)}")
            raise
    
    def write_processed_transactions(self, df: DataFrame, date_partition: str = None) -> None:
        """
        Write processed transaction data to data lake
        
        Args:
            df: Processed transaction DataFrame
            date_partition: Optional date for partitioning
        """
        if date_partition:
            s3_path = f"{self.s3_processed_path}transactions/date={date_partition}/"
        else:
            s3_path = f"{self.s3_processed_path}transactions/"
        
        # Write to S3 partitioned by transaction_date
        self.write_to_s3_parquet(
            df, 
            s3_path, 
            partition_cols=['transaction_date'] if 'transaction_date' in df.columns else None
        )
        
        # Also write to MySQL for operational queries
        try:
            self.write_to_mysql(df, 'processed_transactions', mode='append')
        except Exception as e:
            logger.warning(f"Failed to write to MySQL: {str(e)}")
    
    def write_invalid_transactions(self, df: DataFrame, date_partition: str = None) -> None:
        """
        Write invalid transaction data for audit purposes
        
        Args:
            df: Invalid transaction DataFrame
            date_partition: Optional date for partitioning
        """
        if df.count() == 0:
            logger.info("No invalid transactions to write")
            return
        
        if date_partition:
            s3_path = f"{self.s3_processed_path}invalid_transactions/date={date_partition}/"
        else:
            s3_path = f"{self.s3_processed_path}invalid_transactions/"
        
        self.write_to_s3_parquet(df, s3_path, mode='append')
        
        # Write to MySQL for monitoring
        try:
            self.write_to_mysql(df, 'invalid_transactions', mode='append')
        except Exception as e:
            logger.warning(f"Failed to write invalid transactions to MySQL: {str(e)}")
    
    def write_summaries(self, summaries: Dict[str, DataFrame]) -> None:
        """
        Write all summary DataFrames to storage systems
        
        Args:
            summaries: Dictionary of summary DataFrames
        """
        for summary_name, summary_df in summaries.items():
            if summary_df.count() == 0:
                logger.info(f"No data to write for {summary_name}")
                continue
            
            # Write to S3 Data Lake
            s3_path = f"{self.s3_summary_path}{summary_name}/"
            self.write_to_s3_parquet(
                summary_df, 
                s3_path, 
                partition_cols=['transaction_date'] if 'transaction_date' in summary_df.columns else None
            )
            
            # Write to MySQL for operational reporting
            mysql_table_name = f"summary_{summary_name}"
            try:
                self.write_to_mysql(summary_df, mysql_table_name, mode='append')
            except Exception as e:
                logger.warning(f"Failed to write {summary_name} to MySQL: {str(e)}")
    
    def write_data_quality_report(self, valid_count: int, invalid_count: int, 
                                 processing_date: str) -> None:
        """
        Write data quality report to both S3 and MySQL
        
        Args:
            valid_count: Number of valid transactions
            invalid_count: Number of invalid transactions
            processing_date: Date of processing
        """
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        
        # Create quality report DataFrame
        quality_data = [(
            processing_date,
            valid_count,
            invalid_count,
            valid_count + invalid_count,
            (valid_count / (valid_count + invalid_count)) * 100 if (valid_count + invalid_count) > 0 else 0
        )]
        
        quality_columns = [
            'processing_date', 'valid_transactions', 'invalid_transactions', 
            'total_transactions', 'data_quality_percentage'
        ]
        
        quality_df = spark.createDataFrame(quality_data, quality_columns)
        
        # Write to S3
        s3_path = f"{self.s3_summary_path}data_quality_reports/"
        self.write_to_s3_parquet(quality_df, s3_path, mode='append')
        
        # Write to MySQL
        try:
            self.write_to_mysql(quality_df, 'data_quality_reports', mode='append')
        except Exception as e:
            logger.warning(f"Failed to write quality report to MySQL: {str(e)}")
    
    def create_mysql_tables(self) -> None:
        """
        Create necessary MySQL tables if they don't exist
        Note: This is a simplified version. In production, use proper DDL scripts.
        """
        logger.info("MySQL table creation should be handled by DBA scripts")
        logger.info("Tables needed: processed_transactions, invalid_transactions, summary_*, data_quality_reports")
    
    def optimize_s3_storage(self, s3_path: str) -> None:
        """
        Optimize S3 storage by compacting small files
        Note: This would typically be handled by a separate job
        
        Args:
            s3_path: S3 path to optimize
        """
        logger.info(f"Storage optimization for {s3_path} should be handled by maintenance jobs")
    
    def archive_old_data(self, cutoff_date: str) -> None:
        """
        Archive old data to cheaper storage tiers
        
        Args:
            cutoff_date: Date before which data should be archived
        """
        logger.info(f"Data archival before {cutoff_date} should be handled by lifecycle policies")