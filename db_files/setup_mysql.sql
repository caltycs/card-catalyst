-- MySQL setup script for transaction processing pipeline
-- Create database
CREATE DATABASE IF NOT EXISTS catalyst;
USE catalyst;

-- Create processed_transactions table
CREATE TABLE IF NOT EXISTS processed_transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    merchant_id VARCHAR(50) NOT NULL,
    amount DECIMAL(15,2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    customer_id_hash VARCHAR(64),
    location VARCHAR(100),
    transaction_date DATE NOT NULL,
    transaction_hour INT,
    completeness_score DECIMAL(3,2),
    transaction_hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_merchant_date (merchant_id, transaction_date),
    INDEX idx_transaction_date (transaction_date),
    INDEX idx_timestamp (timestamp)
);

-- Create invalid_transactions table for audit
CREATE TABLE IF NOT EXISTS invalid_transactions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    transaction_id VARCHAR(50),
    merchant_id VARCHAR(50),
    card_number VARCHAR(20),
    amount DECIMAL(15,2),
    currency VARCHAR(3),
    timestamp TIMESTAMP NULL,
    customer_id VARCHAR(50),
    location VARCHAR(100),
    transaction_date DATE,
    validation_errors TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_validation_date (transaction_date),
    INDEX idx_created_at (created_at)
);

-- Create daily merchant summary table
CREATE TABLE IF NOT EXISTS summary_daily_merchant_summary (
    merchant_id VARCHAR(50),
    transaction_date DATE,
    total_transactions INT NOT NULL DEFAULT 0,
    total_amount DECIMAL(15,2) NOT NULL DEFAULT 0.00,
    avg_transaction_amount DECIMAL(10,2),
    min_transaction_amount DECIMAL(10,2),
    max_transaction_amount DECIMAL(10,2),
    std_transaction_amount DECIMAL(10,2),
    total_customers INT DEFAULT 0,
    currencies_used JSON,
    locations JSON,
    active_hours JSON,
    currency_count INT DEFAULT 0,
    location_count INT DEFAULT 0,
    active_hour_count INT DEFAULT 0,
    avg_data_quality_score DECIMAL(3,2),
    first_transaction_time TIMESTAMP NULL,
    last_transaction_time TIMESTAMP NULL,
    summary_generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (merchant_id, transaction_date),
    INDEX idx_transaction_date (transaction_date),
    INDEX idx_total_amount (total_amount),
    INDEX idx_total_transactions (total_transactions)
);

-- Create risk metrics table
CREATE TABLE IF NOT EXISTS summary_risk_metrics (
    merchant_id VARCHAR(50),
    transaction_date DATE,
    high_value_transactions INT DEFAULT 0,
    very_high_value_transactions INT DEFAULT 0,
    transaction_velocity INT DEFAULT 0,
    max_single_transaction DECIMAL(15,2),
    max_transaction_ratio DECIMAL(5,4),
    unique_locations INT DEFAULT 0,
    unique_customers INT DEFAULT 0,
    risk_score INT DEFAULT 0,
    risk_level VARCHAR(10) DEFAULT 'LOW',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (merchant_id, transaction_date),
    INDEX idx_risk_level (risk_level),
    INDEX idx_risk_score (risk_score),
    INDEX idx_transaction_date (transaction_date)
);

-- Create hourly patterns table
CREATE TABLE IF NOT EXISTS summary_hourly_patterns (
    merchant_id VARCHAR(50),
    transaction_date DATE,
    transaction_hour INT,
    hourly_transactions INT DEFAULT 0,
    hourly_amount DECIMAL(15,2) DEFAULT 0.00,
    hourly_avg_amount DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (merchant_id, transaction_date, transaction_hour),
    INDEX idx_transaction_hour (transaction_hour),
    INDEX idx_hourly_amount (hourly_amount),
    CONSTRAINT chk_hour_range CHECK (transaction_hour >= 0 AND transaction_hour <= 23)
);

-- Create currency breakdown table
CREATE TABLE IF NOT EXISTS summary_currency_breakdown (
    merchant_id VARCHAR(50),
    transaction_date DATE,
    currency VARCHAR(3),
    currency_transactions INT DEFAULT 0,
    currency_amount DECIMAL(15,2) DEFAULT 0.00,
    currency_avg_amount DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (merchant_id, transaction_date, currency),
    INDEX idx_currency (currency),
    INDEX idx_currency_amount (currency_amount),
    INDEX idx_transaction_date (transaction_date)
);

-- Create data quality reports table
CREATE TABLE IF NOT EXISTS data_quality_reports (
    processing_date DATE PRIMARY KEY,
    valid_transactions INT NOT NULL DEFAULT 0,
    invalid_transactions INT NOT NULL DEFAULT 0,
    total_transactions INT NOT NULL DEFAULT 0,
    data_quality_percentage DECIMAL(5,2) NOT NULL DEFAULT 0.00,
    processing_duration_seconds INT,
    errors_encountered TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_quality_percentage (data_quality_percentage),
    INDEX idx_processing_date (processing_date),
    CONSTRAINT chk_quality_percentage CHECK (data_quality_percentage >= 0 AND data_quality_percentage <= 100)
);

-- Create merchant performance KPIs table
CREATE TABLE IF NOT EXISTS summary_merchant_kpis (
    merchant_id VARCHAR(50),
    transaction_date DATE,
    transaction_count INT DEFAULT 0,
    gross_volume DECIMAL(15,2) DEFAULT 0.00,
    average_ticket_size DECIMAL(10,2),
    revenue_per_transaction DECIMAL(10,2),
    customer_count INT DEFAULT 0,
    revenue_per_customer DECIMAL(10,2),
    active_locations INT DEFAULT 0,
    operating_hours INT DEFAULT 0,
    data_quality_score DECIMAL(3,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (merchant_id, transaction_date),
    INDEX idx_gross_volume (gross_volume),
    INDEX idx_customer_count (customer_count),
    INDEX idx_transaction_count (transaction_count),
    INDEX idx_transaction_date (transaction_date)
);

-- Create processing logs table for pipeline monitoring
CREATE TABLE IF NOT EXISTS pipeline_processing_logs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    processing_date DATE NOT NULL,
    pipeline_mode VARCHAR(20) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'RUNNING',
    records_processed INT DEFAULT 0,
    records_valid INT DEFAULT 0,
    records_invalid INT DEFAULT 0,
    error_message TEXT,
    processing_duration_seconds INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_processing_date (processing_date),
    INDEX idx_status (status),
    INDEX idx_start_time (start_time),
    CONSTRAINT chk_status CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'PARTIAL'))
);

-- Create additional indexes for better query performance
CREATE INDEX idx_processed_transactions_merchant_amount ON processed_transactions(merchant_id, amount);
CREATE INDEX idx_processed_transactions_location ON processed_transactions(location);
CREATE INDEX idx_processed_transactions_currency ON processed_transactions(currency);
CREATE INDEX idx_invalid_transactions_merchant ON invalid_transactions(merchant_id);

-- Create views for common queries
CREATE VIEW daily_transaction_summary AS
SELECT 
    transaction_date,
    COUNT(*) as total_transactions,
    SUM(amount) as total_volume,
    AVG(amount) as avg_transaction_size,
    COUNT(DISTINCT merchant_id) as active_merchants,
    COUNT(DISTINCT location) as active_locations,
    COUNT(DISTINCT currency) as currencies_used
FROM processed_transactions 
GROUP BY transaction_date
ORDER BY transaction_date DESC;

-- Create view for merchant performance overview
CREATE VIEW merchant_performance_overview AS
SELECT 
    m.merchant_id,
    m.transaction_date,
    m.total_transactions,
    m.total_amount,
    m.avg_transaction_amount,
    r.risk_level,
    r.risk_score,
    CASE 
        WHEN m.total_transactions > 100 AND m.total_amount > 10000 THEN 'HIGH_VOLUME'
        WHEN m.total_transactions > 50 AND m.total_amount > 5000 THEN 'MEDIUM_VOLUME'
        ELSE 'LOW_VOLUME'
    END as volume_category
FROM summary_daily_merchant_summary m
LEFT JOIN summary_risk_metrics r ON m.merchant_id = r.merchant_id AND m.transaction_date = r.transaction_date
ORDER BY m.transaction_date DESC, m.total_amount DESC;

-- Create view for data quality monitoring
CREATE VIEW data_quality_monitoring AS
SELECT 
    processing_date,
    total_transactions,
    valid_transactions,
    invalid_transactions,
    data_quality_percentage,
    CASE 
        WHEN data_quality_percentage >= 95 THEN 'EXCELLENT'
        WHEN data_quality_percentage >= 90 THEN 'GOOD'
        WHEN data_quality_percentage >= 85 THEN 'ACCEPTABLE'
        ELSE 'NEEDS_ATTENTION'
    END as quality_status
FROM data_quality_reports
ORDER BY processing_date DESC;

-- Insert some reference data if needed
INSERT IGNORE INTO summary_daily_merchant_summary (merchant_id, transaction_date, total_transactions, total_amount)
VALUES ('SYSTEM_TEST', CURDATE(), 0, 0.00);

-- Create stored procedures for common operations
DELIMITER //

-- Procedure to get merchant summary for date range
CREATE PROCEDURE GetMerchantSummary(
    IN p_merchant_id VARCHAR(50),
    IN p_start_date DATE,
    IN p_end_date DATE
)
BEGIN
    SELECT 
        merchant_id,
        transaction_date,
        total_transactions,
        total_amount,
        avg_transaction_amount,
        total_customers,
        currency_count,
        location_count
    FROM summary_daily_merchant_summary
    WHERE merchant_id = p_merchant_id 
    AND transaction_date BETWEEN p_start_date AND p_end_date
    ORDER BY transaction_date;
END //

-- Procedure to cleanup old data (retention policy)
CREATE PROCEDURE CleanupOldData(
    IN p_retention_days INT
)
BEGIN
    DECLARE v_cutoff_date DATE;
    SET v_cutoff_date = DATE_SUB(CURDATE(), INTERVAL p_retention_days DAY);
    
    -- Clean up old invalid transactions (keep for audit)
    DELETE FROM invalid_transactions 
    WHERE created_at < DATE_SUB(NOW(), INTERVAL p_retention_days DAY);
    
    -- Clean up old processing logs
    DELETE FROM pipeline_processing_logs 
    WHERE created_at < DATE_SUB(NOW(), INTERVAL p_retention_days DAY);
    
    -- Note: Don't delete processed_transactions and summaries for historical analysis
    
    SELECT CONCAT('Cleaned up data older than ', p_retention_days, ' days') as result;
END //

DELIMITER ;

-- Grant permissions for pipeline user (adjust username as needed)
-- Note: Run these manually after creating your pipeline user
-- GRANT SELECT, INSERT, UPDATE, DELETE ON catalyst.* TO 'pipeline_user'@'localhost';
-- GRANT EXECUTE ON PROCEDURE catalyst.GetMerchantSummary TO 'pipeline_user'@'localhost';
-- GRANT EXECUTE ON PROCEDURE catalyst.CleanupOldData TO 'pipeline_user'@'localhost';
-- FLUSH PRIVILEGES;

-- Show table creation summary
SELECT 
    table_name,
    table_comment,
    table_rows,
    data_length,
    index_length
FROM information_schema.tables 
WHERE table_schema = 'catalyst'
ORDER BY table_name;

-- Show created views
SELECT table_name as view_name
FROM information_schema.views 
WHERE table_schema = 'catalyst';

-- Show created stored procedures
SELECT routine_name as procedure_name
FROM information_schema.routines 
WHERE routine_schema = 'catalyst' AND routine_type = 'PROCEDURE';

-- Final setup completion message
SELECT 'Transaction processing database setup completed successfully!' as status;