"""
Configuration file for PySpark transaction processing pipeline
"""

import os
from dotenv import load_dotenv
from typing import Dict


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
ENV_PATH = os.path.join(BASE_DIR, ".env")

# Load environment variables
load_dotenv(dotenv_path=ENV_PATH)

class Config:
    """Configuration class containing all necessary settings"""

    # AWS S3 Configuration
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    #assert AWS_ACCESS_KEY_ID is not None, "AWS_ACCESS_KEY_ID is not loaded!"

    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')

    # S3 Paths
    S3_BUCKET = os.getenv("S3_BUCKET", "daily-transaction-store")
    S3_RAW_PATH = f"s3a://{S3_BUCKET}/raw/transaction_type=purchase/"
    S3_PROCESSED_PATH = f"s3a://{S3_BUCKET}/processed/"
    S3_SUMMARY_PATH = f"s3a://{S3_BUCKET}/summaries/"

    # MySQL Configuration
    MYSQL_CONFIG = {
        'host': os.getenv('MYSQL_HOST', 'localhost'),
        'port': int(os.getenv('MYSQL_PORT', '3306')),
        'database': os.getenv('MYSQL_DATABASE', 'transaction_db'),
        'user': os.getenv('MYSQL_USER', 'admin'),
        'password': os.getenv('MYSQL_PASSWORD', 'password'),
        'driver': 'com.mysql.cj.jdbc.Driver'
    }

    # Spark Configuration
    SPARK_CONFIG = {
        'app_name': 'CardCatalyst',
        'master': os.getenv('SPARK_MASTER', 'local[*]'),
        'config': {
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.hadoop.fs.s3a.access.key': AWS_ACCESS_KEY_ID,
            'spark.hadoop.fs.s3a.secret.key': AWS_SECRET_ACCESS_KEY,
            'spark.hadoop.fs.s3a.endpoint': f's3.{AWS_REGION}.amazonaws.com',
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.jars.packages': os.getenv(
                "SPARK_JARS_PACKAGES",
                "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367,mysql:mysql-connector-java:8.0.33"
            )
        }
    }

    # Data Validation Rules
    VALIDATION_RULES = {
        'amount': {'min': 0.01, 'max': 10000.0},
        'card_number_length': 16,
        'required_fields': [
            'transaction_id', 'merchant_id', 'card_number',
            'amount', 'timestamp', 'customer_id',
            'currency', 'location'
        ]
    }

    # Masking Configuration
    MASKING_CONFIG = {
        'card_number_mask_char': '*',
        'card_number_visible_digits': 4  # Last 4 digits visible
    }

def get_mysql_jdbc_url() -> str:
    """Generate MySQL JDBC URL from configuration"""
    mysql_config = Config.MYSQL_CONFIG
    return f"jdbc:mysql://{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}"

def get_mysql_properties() -> Dict[str, str]:
    """Get MySQL connection properties for Spark"""
    mysql_config = Config.MYSQL_CONFIG
    return {
        'user': mysql_config['user'],
        'password': mysql_config['password'],
        'driver': mysql_config['driver']
    }
