-- MySQL setup script for transaction processing pipeline
-- Create database
CREATE DATABASE IF NOT EXISTS transaction_db;
USE transaction_db;

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
    transaction_id VARCHAR(50),
    merchant_id VARCHAR(50),
    card_number VARCHAR(20),
    amount DECIMAL(15,2),
    currency VARCHAR(3),
    timestamp TIMESTAMP,
    customer_id VARCHAR(50),
    location VARCHAR(100),
    transaction_date DATE,
    validation_errors TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_validation_date (transaction_date)
);

-- Create daily merchant summary table
CREATE TABLE IF NOT EXISTS summary_daily_merchant_summary (
    merchant_id VARCHAR(50),
    transaction_date DATE,
    total_transactions INT NOT NULL,
    total_amount DECIMAL(15,2) NOT NULL,
    avg_transaction_amount DECIMAL(10,2),
    min_transaction_amount DECIMAL(10,2),
    max_transaction_amount DECIMAL(10,2),
    std_transaction_amount DECIMAL(10,2),
    total_customers INT,
    currencies_used JSON,
    locations JSON,
    active_hours JSON,
    currency_count INT,
    location_count INT,
    active_hour_count INT,
    avg_data_quality_score DECIMAL(3,2),
    first_transaction_time TIMESTAMP,
    last_transaction_time TIMESTAMP,
    summary_generated_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (merchant_id, transaction_date),
    INDEX idx_transaction_date (transaction_date),
    INDEX idx_total_amount (total_amount)
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
    unique_locations INT,
    unique_customers INT,
    risk_score INT,
    risk_level VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (merchant_id, transaction_date),
    INDEX idx_risk_level (risk_level),
    INDEX idx_risk_score (risk_score)
);

-- Create hourly patterns table
CREATE TABLE IF NOT EXISTS summary_hourly_patterns (
    merchant_id VARCHAR(50),
    transaction_date DATE,
    transaction_hour INT,
    hourly_transactions INT,
    hourly_amount DECIMAL(15,2),
    hourly_avg_amount DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (merchant_id, transaction_date, transaction_hour),
    INDEX idx_transaction_hour (transaction_hour)
);

-- Create currency breakdown table
CREATE TABLE IF NOT EXISTS summary_currency_breakdown (
    merchant_id VARCHAR(50),
    transaction_date DATE,
    currency VARCHAR(3),
    currency_