"""
Main processing pipeline for transaction data processing
"""

import sys
import argparse
import logging
import os

# Import all modules
from utilities.spark_session import spark_manager
from extract.data_reader import DataReader
from transform.data_cleaner import DataCleaner
from transform.data_masker import DataMasker
from transform.summary_calculator import SummaryCalculator
from load.data_writer import DataWriter

logging.basicConfig(
    filename="pipeline.log",
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TransactionProcessor:
    """Main transaction processing pipeline"""
    
    def __init__(self):
        self.spark = spark_manager.get_spark_session()
        self.data_reader = DataReader(self.spark)
        self.data_cleaner = DataCleaner()
        self.data_masker = DataMasker()
        self.summary_calculator = SummaryCalculator()
        self.data_writer = DataWriter()
    
    def process_transactions(self, year: str , month: str , day: str , mode: str) -> dict:
        """
        Complete transaction processing pipeline
        
        Args:
            year: Year to process (optional)
            month: Month to process (optional)
            day: Day to process (optional)
            mode : s3 or local
        Returns:
            Dictionary with processing results
        """
        logger.info("Starting transaction processing pipeline...")
        
        try:
            # Step 1: Read data from S3 or Local
            logger.info(f"Step 1: Reading transaction data from {mode} for date {year}-{month}-{day}...")
            processing_date = f"{year}-{month}-{day}"
            raw_df = self.data_reader.read_transactions_by_date(year, month, day, mode)
            raw_count = raw_df.count()
            # Step 2: Clean and validate data
            logger.info("Step 2: Cleaning and validating transaction data...")
            valid_df, invalid_df = self.data_cleaner.clean_and_validate(raw_df)
            valid_count = valid_df.count()
            invalid_count = invalid_df.count()
            logger.info(f"Validation results: {valid_count} valid, {invalid_count} invalid transactions")

            # Step 3: Apply data masking
            logger.info("Step 3: Applying data masking for sensitive fields...")
            masked_df = self.data_masker.apply_all_masking(valid_df)

            # Step 4: Calculate summaries
            logger.info("Step 4: Calculating daily merchant summaries...")
            summaries = self.summary_calculator.generate_comprehensive_summary(valid_df)
            
            # Step 5: Store results
            logger.info(f"Step 5: Storing results to data lake and RDBMS...")
            
            # Write processed transactions
            self.data_writer.write_processed_transactions(masked_df, processing_date, mode)
            
            # Write invalid transactions for audit
            if invalid_count > 0:
                self.data_writer.write_invalid_transactions(invalid_df, processing_date, mode)

            # Write summaries
            self.data_writer.write_summaries(summaries, processing_date, mode)
            
            # Write data quality report
            self.data_writer.write_data_quality_report(valid_count, invalid_count, processing_date, mode)
            
            # Unpersist cached DataFrames to free memory
            for summary_df in summaries.values():
                summary_df.unpersist()
            
            logger.info("Transaction processing pipeline completed successfully!")
            
            return {
                'processing_date': processing_date,
                'raw_count': raw_count,
                'valid_count': valid_count,
                'invalid_count': invalid_count,
                'data_quality_percentage': (valid_count / raw_count) * 100 if raw_count > 0 else 0,
                'summaries_generated': len(summaries)
            }
            
        except Exception as e:
            logger.error(f"Error in transaction processing pipeline: {str(e)}")
            raise


    def generate_processing_report(self, results: dict) -> None:
        """
        Generate and log processing report
        
        Args:
            results: Processing results dictionary
        """
        logger.info("="*60)
        logger.info("TRANSACTION PROCESSING REPORT")
        logger.info("="*60)
        logger.info(f"Processing Date: {results.get('processing_date', 'N/A')}")
        logger.info(f"Raw Transactions: {results.get('raw_count', 0):,}")
        logger.info(f"Valid Transactions: {results.get('valid_count', 0):,}")
        logger.info(f"Invalid Transactions: {results.get('invalid_count', 0):,}")
        logger.info(f"Data Quality: {results.get('data_quality_percentage', 0):.2f}%")
        logger.info(f"Summaries Generated: {results.get('summaries_generated', 0)}")
        logger.info("="*60)

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Transaction Processing Pipeline')
    parser.add_argument('--mode', required= True, type=str, help='s3 or local')
    parser.add_argument('--year', required=True, type=str, help='Year to  process (YYYY)')
    parser.add_argument('--month', required=True, type=str, help='Month to process (MM)')
    parser.add_argument('--day', required=True, type=str, help='Day to process (DD)')
    args = parser.parse_args()
    
    processor = None
    try:
        if args.year and args.month and args.day and args.mode:
            # Daily processing mode
            processor = TransactionProcessor()
            logger.info(f"Data processing for {args.mode} mode {args.year}-{args.month}-{args.day}")
            results = processor.process_transactions(args.year, args.month, args.day, args.mode)
            processor.generate_processing_report(results)
        else:
            logger.info("All required arguments not passed")
            sys.exit(1)
        logger.info("Pipeline execution completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        sys.exit(1)
    
    finally:
        # Clean up Spark session
        if processor:
            spark_manager.stop_spark_session()

if __name__ == "__main__":
    main()