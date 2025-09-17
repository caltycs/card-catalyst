# card-catalyst

spark-submit --master local[*] --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367,mysql:mysql-connector-java:8.0.33 src/main.py --mode s3 --year 2026 --month 06 --day 24

# credentials.env
# AWS Configuration
AWS_ACCESS_KEY_ID=<>
AWS_SECRET_ACCESS_KEY=<>
AWS_REGION=us-east-1

# MySQL Configuration
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DATABASE=catalyst
MYSQL_USER=<>
MYSQL_PASSWORD=<>

# Spark Configuration (Optional - will use defaults if not set)
SPARK_MASTER=local[*]
SPARK_APP_NAME=TransactionProcessor

# Logging Configuration
LOG_LEVEL=INFO