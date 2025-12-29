# src/database/schema.sql
-- Tạo bảng trong cơ sở dữ liệu PostgreSQL để lưu trữ giao dịch và kết

-- Bảng lưu trữ TẤT CẢ giao dịch
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id SERIAL PRIMARY KEY,
    step INT,
    type VARCHAR(20),
    amount FLOAT,
    nameOrig VARCHAR(50),
    oldbalanceOrg FLOAT,
    newbalanceOrig FLOAT,
    nameDest VARCHAR(50),
    oldbalanceDest FLOAT,
    newbalanceDest FLOAT,
    isFraud INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bảng lưu kết quả dự báo của AI
CREATE TABLE IF NOT EXISTS fraud_logs (
    id SERIAL PRIMARY KEY,
    transaction_id INT, -- Link với bảng transactions nếu cần
    amount FLOAT,
    anomaly_score FLOAT,
    is_predicted_fraud BOOLEAN,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);