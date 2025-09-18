# card-catalyst

A high-performance PySpark-based transaction processing pipeline for cleaning, validating, masking, and analyzing financial transaction data. Processes data from AWS S3 and stores results in both S3 Data Lake and MySQL RDBMS.

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Apache Spark 3.5.0+
- Java 8 or 11
- MySQL 8.0+
- AWS Account with S3 access

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd card-catalyst

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set up MySQL database
mysql -u root -p < setup_mysql.sql

# Configure environment
cp credentials.env.example credentials.env
# Edit credentials.env with your actual values
```

### Configuration

Create a `credentials.env` file in the project root:

```bash
# AWS Configuration
AWS_ACCESS_KEY_ID=<your_aws_access_key>
AWS_SECRET_ACCESS_KEY=<your_aws_secret_key>
AWS_REGION=us-east-1

# MySQL Configuration
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DATABASE=catalyst
MYSQL_USER=<your_mysql_user>
MYSQL_PASSWORD=<your_mysql_password>

# Spark Configuration (Optional - will use defaults if not set)
SPARK_MASTER=local[*]
SPARK_APP_NAME=TransactionProcessor

# Logging Configuration
LOG_LEVEL=INFO
```

## 📋 Usage

### Spark Submit Command

```bash
spark-submit \
  --master local[*] \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367,mysql:mysql-connector-java:8.0.33 \
  src/main.py \
  --mode s3 \
  --year 2025 \
  --month 06 \
  --day 24
```


### Alternative Execution Methods

#### Using Shell Wrapper (Recommended)
```bash
# Make executable
chmod +x run_pipeline.sh

# Daily processing
./run_pipeline.sh --mode s3 --year 2025 --month 06 --day 24

# With logging
./run_pipeline.sh --mode s3 --year 2025 --month 06 --day 24 --log-file pipeline.log
```


## 🏗️ Architecture

```
Raw Data (S3) → PySpark Processing → Clean/Masked Data → Storage (S3/Local + MySQL)
                       ↓
               Daily Merchant Summaries
```

### Data Flow
1. **Ingestion**: Reads JSON transaction data from S3
2. **Cleaning**: Handles nulls, formatting, standardization
3. **Validation**: Applies business rules and data quality checks
4. **Masking**: Protects sensitive information (PCI-compliant)
5. **Analytics**: Generates merchant summaries and risk metrics
6. **Storage**: Saves to S3 Data Lake and MySQL RDBMS



## 💾 Data Schema

### Input Transaction Format
```json
{
  "transaction_id": "txn1000",
  "merchant_id": "merch_012", 
  "card_number": "4111111111111000",
  "amount": 684.01,
  "timestamp": "2025-06-24T00:54:29Z",
  "customer_id": "cust_1000",
  "currency": "USD",
  "location": "Parkerburg"
}
```

### S3 Data Structure
```
s3://daily-transaction-store/
├── raw/transaction_type=purchase/
│   └── year=2025/month=06/day=24/*.json
├── processed/transactions/
└── summaries/
    ├── daily_merchant_summary/
    ├── risk_metrics/
    ├── hourly_patterns/
    └── currency_breakdown/
```

## 🔒 Security Features

- **PCI-Compliant Data Masking**: Card numbers masked to show only last 4 digits
- **Customer ID Hashing**: SHA-256 hashing for privacy protection
- **Environment-Based Access Control**: Different security levels per environment
- **Audit Trail**: Complete processing history and invalid transaction tracking

## 📊 Analytics & Reporting

### Generated Summaries
1. **Daily Merchant Summary**: Transaction counts, amounts, customer metrics
2. **Risk Metrics**: High-value transaction alerts, velocity indicators
3. **Hourly Patterns**: Transaction distribution throughout the day
4. **Currency Breakdown**: Multi-currency transaction analysis
5. **Data Quality Reports**: Daily completeness and validation metrics

### MySQL Tables Created
- `processed_transactions` - Clean, masked transaction data
- `invalid_transactions` - Failed validation records
- `summary_daily_merchant_summary` - Daily merchant aggregations
- `summary_risk_metrics` - Risk assessment data
- `data_quality_reports` - Processing quality metrics


### Adding New Validations
1. Update `Config.VALIDATION_RULES` in `config.py`
2. Add validation logic in `DataCleaner.validate_transaction_data()`
3. Test with sample invalid data

## 🚨 Troubleshooting

### Common Issues

#### 1. JDBC Type Errors
```bash
Error: Can't get JDBC type for array<string>
```
**Solution**: Use `SafeDataWriter` which converts complex types to JSON strings for MySQL

#### 2. MySQL Connection Issues
```bash
Error: Access denied for user 'de-user'@'localhost'
```
**Solution**:
- Reset MySQL root password
- Update `credentials.env` with correct credentials
- Ensure MySQL service is running: `brew services start mysql`

#### 3. AWS Credentials
```bash
Error: The AWS Access Key Id you provided does not exist
```
**Solution**:
- Verify AWS credentials in `credentials.env`
- Check IAM permissions for S3 access
- Ensure credentials are exported: `export $(grep -v '^#' credentials.env | xargs)`

#### 4. Schema Validation Errors
```bash
Error: is_valid column is always null
```
**Solution**: Use `SafeDataCleaner` which handles different data types properly

### Debug Mode
```bash
# Enable debug logging
LOG_LEVEL=DEBUG python main.py --mode daily --year 2025 --month 06 --day 24

# Show DataFrame schemas
python debug_schema.py
```

## 📈 Performance Optimization

- **Partitioning**: Data partitioned by date for optimal queries
- **Caching**: Strategic DataFrame caching for reuse
- **Coalescing**: Reduces small file problems in S3
- **Batch Processing**: Optimized batch sizes for MySQL writes
- **Dynamic Resource Allocation**: Configurable Spark resources

## 🔄 Production Deployment

### Cluster Mode
```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 8g \
  --driver-memory 4g \
  --executor-cores 4 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367,mysql:mysql-connector-java:8.0.33 \
  main.py \
  --mode daily --year 2025 --month 06 --day 24
```

### Scheduling with Airflow
```python
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

dag = DAG('card_catalyst_daily', schedule_interval='@daily')

process_transactions = BashOperator(
    task_id='process_daily_transactions',
    bash_command='spark-submit --master yarn --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367,mysql:mysql-connector-java:8.0.33 /path/to/main.py --mode daily --year {{ ds_nodash[:4] }} --month {{ ds_nodash[4:6] }} --day {{ ds_nodash[6:8] }}',
    dag=dag
)
```

## 📜 License

This project is licensed under the MIT License - see the LICENSE file for details.


## 📞 Support

For issues and questions:
- Check the troubleshooting section above
- Review logs for detailed error messages
- Create an issue in the repository with relevant details
- Email : info.caltycs@gmail.com

---

**card-catalyst** - Transforming transaction data at scale with Apache Spark 🚀
