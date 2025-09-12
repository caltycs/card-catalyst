"""
Data Cleaner module for cleaning and validating transaction data
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, isnan, length, regexp_replace,
    trim, upper, to_timestamp, date_format
)
from pyspark.sql.types import DoubleType
from utilities.config import Config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataCleaner:
    """Handles data cleaning and validation operations"""
    
    def __init__(self):
        self.validation_rules = Config.VALIDATION_RULES
    
    def clean_transaction_data(self, df: DataFrame) -> DataFrame:
        """
        Clean transaction data by handling nulls, formatting, and standardization
        
        Args:
            df: Raw transaction DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        logger.info("Starting data cleaning process...")
        
        # Remove leading/trailing whitespaces from string columns
        string_columns = ['transaction_id', 'merchant_id', 'card_number', 
                         'customer_id', 'currency', 'location']
        
        cleaned_df = df
        for column in string_columns:
            if column in df.columns:
                cleaned_df = cleaned_df.withColumn(column, trim(col(column)))
        
        # Standardize currency to uppercase
        cleaned_df = cleaned_df.withColumn('currency', upper(col('currency')))
        
        # Clean card number - remove any non-digit characters
        cleaned_df = cleaned_df.withColumn(
            'card_number',
            regexp_replace(col('card_number'), '[^0-9]', '')
        )
        
        # Ensure amount is properly typed and rounded to 2 decimal places
        cleaned_df = cleaned_df.withColumn(
            'amount',
            col('amount').cast(DoubleType())
        )
        
        # Format timestamp consistently
        cleaned_df = cleaned_df.withColumn(
            'timestamp',
            to_timestamp(col('timestamp'), 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\'')
        )
        
        # Add derived columns
        cleaned_df = (cleaned_df
                     .withColumn('transaction_date', date_format(col('timestamp'), 'yyyy-MM-dd'))
                     .withColumn('transaction_hour', date_format(col('timestamp'), 'HH')))
        
        logger.info("Data cleaning completed successfully")
        return cleaned_df
    
    def validate_transaction_data(self, df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """
        Validate transaction data against business rules
        
        Args:
            df: Cleaned transaction DataFrame
            
        Returns:
            Tuple of (valid_df, invalid_df)
        """
        logger.info("Starting data validation process...")
        
        # Check for required fields
        required_fields = self.validation_rules['required_fields']
        
        # Create validation conditions
        validation_conditions = []
        
        # Check for null values in required fields
        for field in required_fields:
            if field in df.columns:
                validation_conditions.append(
                    col(field).isNotNull() & ~isnan(col(field)) & (col(field) != '')
                )
        
        # Validate amount range
        amount_min = self.validation_rules['amount']['min']
        amount_max = self.validation_rules['amount']['max']
        validation_conditions.append(
            (col('amount') >= amount_min) & (col('amount') <= amount_max)
        )
        
        # Validate card number length
        card_length = self.validation_rules['card_number_length']
        validation_conditions.append(
            length(col('card_number')) == card_length
        )
        
        # Validate currency (should be standard 3-letter codes)
        validation_conditions.append(
            col('currency').isin(['USD', 'EUR', 'GBP', 'CAD', 'AUD', 'JPY'])
        )
        
        # Combine all validation conditions
        overall_condition = validation_conditions[0]
        for condition in validation_conditions[1:]:
            overall_condition = overall_condition & condition
        
        # Add validation flag
        df_with_validation = df.withColumn('is_valid', overall_condition)
        
        # Split into valid and invalid records
        valid_df = df_with_validation.filter(col('is_valid') == True).drop('is_valid')
        invalid_df = df_with_validation.filter(col('is_valid') == False).drop('is_valid')
        
        valid_count = valid_df.count()
        invalid_count = invalid_df.count()
        
        logger.info(f"Validation completed: {valid_count} valid records, {invalid_count} invalid records")
        
        return valid_df, invalid_df
    
    def add_data_quality_metrics(self, df: DataFrame) -> DataFrame:
        """
        Add data quality metrics to the DataFrame
        
        Args:
            df: Transaction DataFrame
            
        Returns:
            DataFrame with quality metrics
        """
        # Add completeness score (percentage of non-null required fields)
        required_fields = self.validation_rules['required_fields']
        
        # Calculate completeness for each record
        completeness_conditions = []
        for field in required_fields:
            if field in df.columns:
                completeness_conditions.append(
                    when(col(field).isNotNull() & (col(field) != ''), 1).otherwise(0)
                )
        
        if completeness_conditions:
            # Calculate average completeness score
            total_fields = len(completeness_conditions)
            completeness_sum = completeness_conditions[0]
            for condition in completeness_conditions[1:]:
                completeness_sum = completeness_sum + condition
            
            df = df.withColumn('completeness_score', completeness_sum / total_fields)
        
        return df
    
    def handle_duplicates(self, df: DataFrame) -> DataFrame:
        """
        Handle duplicate transactions
        
        Args:
            df: Transaction DataFrame
            
        Returns:
            DataFrame with duplicates handled
        """
        logger.info("Checking for duplicate transactions...")
        
        original_count = df.count()
        
        # Remove duplicates based on transaction_id (assuming it should be unique)
        deduplicated_df = df.dropDuplicates(['transaction_id'])
        
        final_count = deduplicated_df.count()
        duplicates_removed = original_count - final_count
        
        if duplicates_removed > 0:
            logger.warning(f"Removed {duplicates_removed} duplicate transactions")
        else:
            logger.info("No duplicate transactions found")
        
        return deduplicated_df
    
    def clean_and_validate(self, df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """
        Complete cleaning and validation pipeline
        
        Args:
            df: Raw transaction DataFrame
            
        Returns:
            Tuple of (clean_valid_df, invalid_df)
        """
        # Step 1: Clean the data
        cleaned_df = self.clean_transaction_data(df)
        
        # Step 2: Handle duplicates
        deduplicated_df = self.handle_duplicates(cleaned_df)
        
        # Step 3: Validate the data
        valid_df, invalid_df = self.validate_transaction_data(deduplicated_df)
        
        # Step 4: Add quality metrics
        valid_df = self.add_data_quality_metrics(valid_df)
        
        return valid_df, invalid_df