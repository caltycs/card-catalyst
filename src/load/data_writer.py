"""
Data Writer module for storing processed data to S3 and MySQL
"""
from pyspark.sql.types import ArrayType

from pyspark.sql import DataFrame
from pyspark.sql.functions import to_json, col
from utilities.config import Config, get_mysql_jdbc_url, get_mysql_properties
import logging
from typing import Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataWriter:
    """Handles writing data to various storage systems"""
    
    def __init__(self):
        self.s3_processed_path = Config.S3_PROCESSED_PATH
        self.s3_summary_path = Config.S3_SUMMARY_PATH
        self.local_processed_path = Config.LOCAL_PROCESSED_PATH
        self.local_summary_path = Config.LOCAL_SUMMARY_PATH
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
            logger.info(f"Writing data to S3: {s3_path}, {df.head()}")
            
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

    def write_to_local_json(self, df: DataFrame, local_path: str, mode: str = 'overwrite') -> None:
        """
        Write DataFrame to Local in JSON format

        Args:
            df: DataFrame to write
            local_path: Local destination path
            mode: Write mode
        """
        try:
            logger.info(f"Writing JSON data to Local: {local_path}")

            df.coalesce(1).write.mode(mode).json(local_path)

            record_count = df.count()
            logger.info(f"Successfully wrote {record_count} records to Local as JSON: {local_path}")

        except Exception as e:
            logger.error(f"Error writing JSON to S3 {local_path}: {str(e)}")
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
            logger.info(f"Dataframe to write: {df.schema}")
            array_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, ArrayType)]
            logger.info(f"Array columns to convert: {array_cols}")
            for c in array_cols:
                df = df.withColumn(c, to_json(col(c)).cast("string"))
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
    
    def write_processed_transactions(self, df: DataFrame, date_partition: str = None, mode: str = None) -> None:
        """
        Write processed transaction data to data lake
        
        Args:
            df: Processed transaction DataFrame
            date_partition: Optional date for partitioning
            mode: s3 or local
        """
        if mode == "s3":
            processed_path = f"{self.s3_processed_path}transactions/date={date_partition}/"
            logger.info(f"Writing processed transactions...{processed_path}")
            self.write_to_s3_json(
                df,
                processed_path
            )
        else:
            processed_path = f"{self.local_processed_path}transactions/{date_partition}_processed"
            logger.info(f"Writing processed transactions...{processed_path}")
            self.write_to_local_json(
                df,
                processed_path
            )


        # Also write to MySQL for operational queries
        try:
            self.write_to_mysql(df, 'processed_transactions', mode='append')
        except Exception as e:
            logger.warning(f"Failed to write to MySQL: {str(e)}")
    
    def write_invalid_transactions(self, df: DataFrame, date_partition: str = None, mode : str = None) -> None:
        """
        Write invalid transaction data for audit purposes
        
        Args:
            df: Invalid transaction DataFrame
            date_partition: Optional date for partitioning
        """
        if df.count() == 0:
            logger.info("No invalid transactions to write")
            return
        
        if mode == "s3":
            invalid_path = f"{self.s3_processed_path}invalid_transactions/date={date_partition}/"
            self.write_to_s3_json(df, invalid_path, mode='append')
        else:
            invalid_path = f"{self.local_processed_path}invalid_transactions/{date_partition}_invalid"
            self.write_to_local_json(df, invalid_path, mode='append')

        # Write to MySQL for monitoring
        try:
            self.write_to_mysql(df, 'invalid_transactions', mode='append')
        except Exception as e:
            logger.warning(f"Failed to write invalid transactions to MySQL: {str(e)}")
    
    def write_summaries(self, summaries: Dict[str, DataFrame], processing_date : str, mode : str) -> None:
        """
        Write all summary DataFrames to storage systems
        
        Args:
            summaries: Dictionary of summary DataFrames
        """
        for summary_name, summary_df in summaries.items():
            if summary_df.count() == 0:
                logger.info(f"No data to write for {summary_name}")
                continue

            if mode == "s3":
                # Write to S3 Data Lake
                s3_path = f"{self.s3_summary_path}{summary_name}/{processing_date}/"
                self.write_to_s3_json(
                    summary_df,
                    s3_path)
            elif mode == "local":
                local_path = f"{self.local_summary_path}{summary_name}/{processing_date}_summary"
                self.write_to_local_json(
                    summary_df,
                    local_path)
            
            # Write to MySQL for operational reporting
            mysql_table_name = f"summary_{summary_name}"
            try:
                self.write_to_mysql(summary_df, mysql_table_name, mode='append')
            except Exception as e:
                logger.warning(f"Failed to write {summary_name} to MySQL: {str(e)}")
    
    def write_data_quality_report(self, valid_count: int, invalid_count: int, 
                                 processing_date: str , mode: str) -> None:
        """
        Write data quality report to both S3 and MySQL
        
        Args:
            valid_count: Number of valid transactions
            invalid_count: Number of invalid transactions
            processing_date: Date of processing
            mode: s3 or local
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
        if mode == "s3":
            s3_path = f"{self.s3_summary_path}data_quality_reports/"
            self.write_to_s3_json(quality_df, s3_path, mode='append')
        elif mode == "local":
            local_path = f"{self.local_summary_path}data_quality_reports/{processing_date}_data_quality"
            self.write_to_local_json(quality_df, local_path, mode='append')
        
        # Write to MySQL
        try:
            self.write_to_mysql(quality_df, 'data_quality_reports', mode='append')
        except Exception as e:
            logger.warning(f"Failed to write quality report to MySQL: {str(e)}")
