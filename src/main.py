"""
Main processing pipeline for transaction data processing
"""

import sys
import argparse
from datetime import datetime
import logging

# Import all modules
from utilities.spark_session import spark_manager
from extract.data_reader import DataReader
from transform.data_cleaner import DataCleaner
from transform.data_masker import DataMasker
from transform.summary_calculator import SummaryCalculator
from load.data_writer import DataWriter
from utilities.config import Config

logging.basicConfig(
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
    
    def process_transactions(self, year: str = None, month: str = None, day: str = None) -> dict:
        """
        Complete transaction processing pipeline
        
        Args:
            year: Year to process (optional)
            month: Month to process (optional)
            day: Day to process (optional)
            
        Returns:
            Dictionary with processing results
        """
        logger.info("Starting transaction processing pipeline...")
        
        try:
            # Step 1: Read data from S3
            logger.info("Step 1: Reading transaction data from S3...")
            if year and month and day:
                raw_df = self.data_reader.read_transactions_by_date(year, month, day)
                processing_date = f"{year}-{month}-{day}"
            else:
                raw_df = self.data_reader.read_transactions_from_s3()
                processing_date = datetime.now().strftime("%Y-%m-%d")
            
            raw_count = raw_df.count()
            logger.info(f"Read {raw_count} raw transactions")
            
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
            logger.info("Step 5: Storing results to data lake and RDBMS...")
            
            # Write processed transactions
            self.data_writer.write_processed_transactions(masked_df, processing_date)
            
            # Write invalid transactions for audit
            if invalid_count > 0:
                self.data_writer.write_invalid_transactions(invalid_df, processing_date)
            
            # Write summaries
            self.data_writer.write_summaries(summaries)
            
            # Write data quality report
            self.data_writer.write_data_quality_report(valid_count, invalid_count, processing_date)
            
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
    
    def process_historical_data(self, start_date: str, end_date: str) -> dict:
        """
        Process historical data for a date range
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            Dictionary with processing results
        """
        logger.info(f"Processing historical data from {start_date} to {end_date}")
        
        # Read data for date range
        raw_df = self.data_reader.read_transactions_date_range(start_date, end_date)
        
        # Process the data
        valid_df, invalid_df = self.data_cleaner.clean_and_validate(raw_df)
        masked_df = self.data_masker.apply_all_masking(valid_df)
        summaries = self.summary_calculator.generate_comprehensive_summary(valid_df)
        
        # Store results
        self.data_writer.write_processed_transactions(masked_df)
        self.data_writer.write_summaries(summaries)
        
        return {
            'date_range': f"{start_date} to {end_date}",
            'raw_count': raw_df.count(),
            'valid_count': valid_df.count(),
            'invalid_count': invalid_df.count()
        }
    
    def validate_data_availability(self, year: str, month: str, day: str) -> bool:
        """
        Validate if data is available for processing
        
        Args:
            year: Year
            month: Month  
            day: Day
            
        Returns:
            Boolean indicating data availability
        """
        s3_path = f"{Config.S3_RAW_PATH}year={year}/month={month}/day={day}/"
        return self.data_reader.validate_data_availability(s3_path)
    
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
    parser.add_argument('--year', type=str, help='Year to  process (YYYY)')
    parser.add_argument('--month', type=str, help='Month to process (MM)')
    parser.add_argument('--day', type=str, help='Day to process (DD)')
    parser.add_argument('--start-date', type=str, help='Start date for historical processing (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date for historical processing (YYYY-MM-DD)')
    parser.add_argument('--validate-only', action='store_true', help='Only validate data availability')
    
    args = parser.parse_args()
    
    processor = None
    try:
        processor = TransactionProcessor()
        
        if args.validate_only:
            # Validation mode
            if args.year and args.month and args.day:
                is_available = processor.validate_data_availability(args.year, args.month, args.day)
                logger.info(f"Data availability for {args.year}-{args.month}-{args.day}: {is_available}")
                sys.exit(0 if is_available else 1)
            else:
                logger.error("Year, month, and day are required for validation")
                sys.exit(1)
        
        elif args.start_date and args.end_date:
            # Historical processing mode
            results = processor.process_historical_data(args.start_date, args.end_date)
            processor.generate_processing_report(results)
        
        else:
            # Regular processing mode
            results = processor.process_transactions(args.year, args.month, args.day)
            processor.generate_processing_report(results)
        
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