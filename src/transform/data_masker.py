"""
Data Masker module for masking sensitive fields in transaction data
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, length, concat, lit, substring, sha2
from src.utilities.config import Config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataMasker:
    """Handles masking of sensitive data fields"""
    
    def __init__(self):
        self.masking_config = Config.MASKING_CONFIG
    
    def mask_card_number(self, df: DataFrame) -> DataFrame:
        """
        Mask card number showing only last 4 digits
        
        Args:
            df: DataFrame with card_number column
            
        Returns:
            DataFrame with masked card_number
        """
        logger.info("Masking card numbers...")
        
        mask_char = self.masking_config['card_number_mask_char']
        visible_digits = self.masking_config['card_number_visible_digits']
        
        masked_df = df.withColumn(
            'card_number_masked',
            when(
                length(col('card_number')) >= visible_digits,
                concat(
                    lit(mask_char * (16 - visible_digits)),  # Assuming 16 digit cards
                    substring(col('card_number'), -visible_digits, visible_digits)
                )
            ).otherwise(lit(mask_char * 16))  # Fully mask if invalid length
        )
        
        return masked_df
    
    def hash_customer_id(self, df: DataFrame) -> DataFrame:
        """
        Hash customer ID for privacy protection while maintaining referential integrity
        
        Args:
            df: DataFrame with customer_id column
            
        Returns:
            DataFrame with hashed customer_id
        """
        logger.info("Hashing customer IDs...")
        
        # Create a SHA-256 hash of customer_id
        hashed_df = df.withColumn(
            'customer_id_hash',
            sha2(col('customer_id'), 256)
        )
        
        return hashed_df
    
    def mask_location(self, df: DataFrame) -> DataFrame:
        """
        Generalize location data for privacy (e.g., city level only)
        
        Args:
            df: DataFrame with location column
            
        Returns:
            DataFrame with generalized location
        """
        logger.info("Generalizing location data...")
        
        # For this example, we'll keep location as-is, but in production you might:
        # - Map to broader geographic regions
        # - Remove specific addresses
        # - Use only city/state level data
        
        generalized_df = df.withColumn(
            'location_generalized',
            col('location')  # Keeping as-is for this example
        )
        
        return generalized_df
    
    def create_transaction_hash(self, df: DataFrame) -> DataFrame:
        """
        Create a unique hash for transaction for audit purposes
        
        Args:
            df: Transaction DataFrame
            
        Returns:
            DataFrame with transaction hash
        """
        logger.info("Creating transaction hashes...")
        
        # Create hash from transaction_id, merchant_id, amount, and timestamp
        hashed_df = df.withColumn(
            'transaction_hash',
            sha2(
                concat(
                    col('transaction_id'),
                    col('merchant_id'),
                    col('amount').cast('string'),
                    col('timestamp').cast('string')
                ),
                256
            )
        )
        
        return hashed_df
    
    def apply_all_masking(self, df: DataFrame) -> DataFrame:
        """
        Apply all masking operations to the DataFrame
        
        Args:
            df: Clean transaction DataFrame
            
        Returns:
            DataFrame with all sensitive fields masked
        """
        logger.info("Applying comprehensive data masking...")
        
        # Apply all masking operations
        masked_df = df
        masked_df = self.mask_card_number(masked_df)
        masked_df = self.hash_customer_id(masked_df)
        masked_df = self.mask_location(masked_df)
        masked_df = self.create_transaction_hash(masked_df)
        
        # Create a version suitable for analytics (with masked sensitive data)
        analytics_df = (masked_df
                       .drop('card_number', 'customer_id')  # Remove original sensitive fields
                       .withColumnRenamed('card_number_masked', 'card_number_masked')
                       .withColumnRenamed('customer_id_hash', 'customer_id_hash')
                       .withColumnRenamed('location_generalized', 'location'))
        
        logger.info("Data masking completed successfully")
        return analytics_df
    
    def create_secure_dataset(self, df: DataFrame) -> DataFrame:
        """
        Create a secure dataset with minimal sensitive information for reporting
        
        Args:
            df: Masked transaction DataFrame
            
        Returns:
            Secure DataFrame suitable for general reporting
        """
        logger.info("Creating secure dataset for reporting...")
        
        # Select only non-sensitive columns for secure reporting
        secure_columns = [
            'transaction_id',
            'merchant_id',
            'amount',
            'currency',
            'timestamp',
            'transaction_date',
            'transaction_hour',
            'location',
            'completeness_score',
            'transaction_hash'
        ]
        
        # Filter to include only columns that exist in the DataFrame
        available_columns = [col for col in secure_columns if col in df.columns]
        
        secure_df = df.select(*available_columns)
        
        return secure_df
    
    def mask_for_environment(self, df: DataFrame, environment: str = 'production') -> DataFrame:
        """
        Apply different masking levels based on environment
        
        Args:
            df: Transaction DataFrame
            environment: Target environment ('development', 'staging', 'production')
            
        Returns:
            DataFrame with environment-appropriate masking
        """
        logger.info(f"Applying masking for {environment} environment...")
        
        if environment.lower() == 'production':
            # Full masking for production
            return self.apply_all_masking(df)
        
        elif environment.lower() == 'staging':
            # Partial masking for staging
            masked_df = df
            masked_df = self.mask_card_number(masked_df)
            masked_df = self.create_transaction_hash(masked_df)
            return masked_df
        
        elif environment.lower() == 'development':
            # Minimal masking for development (still mask card numbers)
            masked_df = df
            masked_df = self.mask_card_number(masked_df)
            return masked_df
        
        else:
            logger.warning(f"Unknown environment: {environment}. Applying full masking.")
            return self.apply_all_masking(df)