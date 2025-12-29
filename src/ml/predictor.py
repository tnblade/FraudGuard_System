# src/ml/predictor.py
# Predictor script để dự báo gian lận sử dụng mô hình Autoencoder đã huấn luyện

import os
import pickle
import numpy as np
import pandas as pd
import tensorflow as tf
from src.core.config import MODEL_PATH, SCALER_PATH

class FraudPredictor:
    def __init__(self):
        self.model = None
        self.scaler = None
        self.model_columns = None
        self.load_artifacts()

    def load_artifacts(self):
        """Load Model .h5, Scaler .pkl và danh sách cột"""
        try:
            if os.path.exists(MODEL_PATH):
                self.model = tf.keras.models.load_model(MODEL_PATH)
                print(f"✅ Loaded Model from {MODEL_PATH}")
            else:
                print(f"⚠️ Model not found at {MODEL_PATH}")

            if os.path.exists(SCALER_PATH):
                with open(SCALER_PATH, 'rb') as f:
                    self.scaler = pickle.load(f)
                print(f"✅ Loaded Scaler from {SCALER_PATH}")
            else:
                print(f"⚠️ Scaler not found at {SCALER_PATH}")
            
            # Load cột mẫu (để xử lý one-hot encoding cho đúng)
            col_path = os.path.join(os.path.dirname(MODEL_PATH), "model_columns.pkl")
            if os.path.exists(col_path):
                with open(col_path, 'rb') as f:
                    self.model_columns = pickle.load(f)
        except Exception as e:
            print(f"❌ Error loading artifacts: {e}")

    def preprocess(self, df):
        """
        Chuẩn bị dữ liệu input giống hệt lúc train
        """
        # 1. One-hot encoding
        df_processed = pd.get_dummies(df, columns=['type'], prefix='type')
        
        # 2. Đảm bảo đủ cột như lúc train (Chống lỗi thiếu cột khi batch nhỏ)
        if self.model_columns:
            # Thêm các cột thiếu (điền 0)
            for col in self.model_columns:
                if col not in df_processed.columns:
                    df_processed[col] = 0
            # Giữ đúng thứ tự cột
            df_processed = df_processed[self.model_columns]
        
        # 3. Scale dữ liệu
        if self.scaler:
            return self.scaler.transform(df_processed)
        return df_processed.values

    def predict(self, df):
        """
        Input: DataFrame chứa batch giao dịch
        Output: DataFrame có thêm cột 'anomaly_score' và 'is_fraud'
        """
        if not self.model or not self.scaler:
            raise ValueError("Model or Scaler not loaded!")

        # Copy để không ảnh hưởng dữ liệu gốc
        result_df = df.copy()

        # Tiền xử lý (Giống lúc train)
        # Chỉ lấy feature cần thiết để đưa vào model
        features_df = result_df.drop(columns=['nameOrig', 'nameDest', 'isFlaggedFraud', 'isFraud', 'step'], errors='ignore')
        
        # Transform
        X_input = self.preprocess(features_df)
        
        # Dự báo (Reconstruction)
        reconstructions = self.model.predict(X_input, verbose=0)
        
        # Tính MSE (Mean Squared Error) -> Đây chính là Anomaly Score
        mse = np.mean(np.power(X_input - reconstructions, 2), axis=1)
        
        # Gán kết quả lại vào DataFrame
        result_df['anomaly_score'] = mse
        
        # Ngưỡng (Threshold) - Có thể điều chỉnh
        # Trong notebook mẫu bạn chọn 0.1 hoặc lấy theo quantile
        THRESHOLD = 0.05 
        result_df['is_fraud_prediction'] = result_df['anomaly_score'] > THRESHOLD
        
        return result_df