"""
Summary Calculator module for computing daily merchant summaries
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, max as spark_max, min as spark_min,
    stddev, first, collect_set, size, when, current_timestamp, lit
)
from pyspark.sql.types import DecimalType
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SummaryCalculator:
    """Calculates daily merchant transaction summaries"""
    
    def __init__(self):
        pass
    
    def calculate_daily_merchant_summary(self, df: DataFrame) -> DataFrame:
        """
        Calculate comprehensive daily merchant summaries
        
        Args:
            df: Clean transaction DataFrame
            
        Returns:
            DataFrame with daily merchant summaries
        """
        logger.info("Calculating daily merchant summaries...")
        
        # Group by merchant_id and transaction_date
        summary_df = (df
                     .groupBy('merchant_id', 'transaction_date')
                     .agg(
                         # Basic transaction metrics
                         count('transaction_id').alias('total_transactions'),
                         spark_sum('amount').alias('total_amount'),
                         avg('amount').alias('avg_transaction_amount'),
                         spark_min('amount').alias('min_transaction_amount'),
                         spark_max('amount').alias('max_transaction_amount'),
                         stddev('amount').alias('std_transaction_amount'),
                         
                         # Customer metrics
                         count('customer_id_hash').alias('total_customers'),
                         
                         # Currency breakdown
                         collect_set('currency').alias('currencies_used'),
                         
                         # Location metrics
                         collect_set('location').alias('locations'),
                         
                         # Time-based metrics
                         collect_set('transaction_hour').alias('active_hours'),
                         
                         # Data quality metrics
                         avg('completeness_score').alias('avg_data_quality_score'),
                         
                         # First and last transaction times
                         spark_min('timestamp').alias('first_transaction_time'),
                         spark_max('timestamp').alias('last_transaction_time')
                     ))
        
        # Add derived columns
        enhanced_summary = (summary_df
                           .withColumn('currency_count', size(col('currencies_used')))
                           .withColumn('location_count', size(col('locations')))
                           .withColumn('active_hour_count', size(col('active_hours')))
                           .withColumn('avg_transaction_amount', 
                                     col('avg_transaction_amount').cast(DecimalType(10, 2)))
                           .withColumn('total_amount', 
                                     col('total_amount').cast(DecimalType(15, 2)))
                           .withColumn('summary_generated_at', current_timestamp()))
        
        record_count = enhanced_summary.count()
        logger.info(f"Generated {record_count} daily merchant summaries")
        
        return enhanced_summary
    
    def calculate_merchant_risk_metrics(self, df: DataFrame) -> DataFrame:
        """
        Calculate risk-related metrics for merchants
        
        Args:
            df: Clean transaction DataFrame
            
        Returns:
            DataFrame with risk metrics
        """
        logger.info("Calculating merchant risk metrics...")
        
        # Calculate risk indicators
        risk_metrics = (df
                       .groupBy('merchant_id', 'transaction_date')
                       .agg(
                           # High-value transaction indicators
                           spark_sum(when(col('amount') > 1000, 1).otherwise(0)).alias('high_value_transactions'),
                           spark_sum(when(col('amount') > 5000, 1).otherwise(0)).alias('very_high_value_transactions'),
                           
                           # Velocity indicators
                           count('transaction_id').alias('transaction_velocity'),
                           
                           # Amount concentration
                           spark_max('amount').alias('max_single_transaction'),
                           (spark_max('amount') / spark_sum('amount')).alias('max_transaction_ratio'),
                           
                           # Geographic spread
                           size(collect_set('location')).alias('unique_locations'),
                           
                           # Customer concentration
                           size(collect_set('customer_id_hash')).alias('unique_customers')
                       ))
        
        # Add risk score calculation (simple scoring mechanism)
        risk_scored = (risk_metrics
                       .withColumn('risk_score',
                                   when(col('very_high_value_transactions') > 5, lit(10))
                                   .when(col('high_value_transactions') > 10, lit(8))
                                   .when(col('transaction_velocity') > 100, lit(6))
                                   .otherwise(lit(0))  # Add a default value
                                   ))