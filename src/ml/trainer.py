# src/ml/trainer.py
# Trainer script để huấn luyện mô hình phát hiện gian lận sử dụng Autoencoder

import pandas as pd
import numpy as np
import glob
import os
import pickle
import tensorflow as tf
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Input, Dense, BatchNormalization
from tensorflow.keras.callbacks import EarlyStopping

# Import config paths từ project của bạn
from src.core.config import MODEL_PATH, SCALER_PATH

def train_model():
    print("🔄 Finding Dataset...")
    # Logic tìm file csv (Ưu tiên Kaggle input, sau đó là local data)
    files = glob.glob("/kaggle/input/paysim1/*.csv") + glob.glob("data/*.csv")
    
    if not files:
        print("❌ Dataset not found! Vui lòng kiểm tra lại đường dẫn.")
        return

    print(f"✅ Found dataset: {files[0]}")
    df = pd.read_csv(files[0])

    # --- 1. PREPROCESSING (Giống trong notebook) ---
    print("🧹 Cleaning & Preprocessing...")
    
    # Loại bỏ các cột không dùng để train
    cols_to_drop = ['nameOrig', 'nameDest', 'isFlaggedFraud']
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])
    
    # One-hot encoding cho cột 'type'
    df = pd.get_dummies(df, columns=['type'], prefix='type')
    
    # Chuyển về float
    df = df.astype(float)

    # Chỉ dùng giao dịch bình thường (Not Fraud) để train Autoencoder
    df_normal = df[df['isFraud'] == 0]
    
    # Bỏ cột label 'isFraud' và 'step' khi đưa vào model
    drop_cols = ['isFraud', 'step']
    X_normal = df_normal.drop(columns=[c for c in drop_cols if c in df_normal.columns])

    # Chia tập Train/Test
    X_train, X_test = train_test_split(X_normal, test_size=0.2, random_state=42)
    
    print(f"📊 Training on {len(X_train)} normal records...")

    # --- 2. SCALING ---
    print("⚖️ Scaling data...")
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # Lưu Scaler để dùng lại khi predict
    # (Đảm bảo thư mục tồn tại)
    os.makedirs(os.path.dirname(SCALER_PATH), exist_ok=True)
    with open(SCALER_PATH, 'wb') as f:
        pickle.dump(scaler, f)
    print(f"💾 Scaler saved at: {SCALER_PATH}")

    # --- 3. MODEL ARCHITECTURE (Autoencoder) ---
    print("🏗️ Building Autoencoder Model...")
    input_dim = X_train_scaled.shape[1]

    input_layer = Input(shape=(input_dim,))
    
    # Encoder
    encoder = Dense(8, activation="tanh")(input_layer)
    encoder = BatchNormalization()(encoder)
    latent_space = Dense(4, activation="tanh")(encoder) # Bottleneck
    
    # Decoder
    decoder = Dense(8, activation="tanh")(latent_space)
    output_layer = Dense(input_dim, activation="linear")(decoder)

    autoencoder = Model(inputs=input_layer, outputs=output_layer)
    autoencoder.compile(optimizer='adam', loss='mean_squared_error')

    # --- 4. TRAINING ---
    print("🚀 Start Training...")
    callback = EarlyStopping(monitor='val_loss', patience=2, restore_best_weights=True)
    
    autoencoder.fit(
        X_train_scaled, X_train_scaled,
        epochs=5, # Demo để nhanh, thực tế có thể tăng lên
        batch_size=2048,
        shuffle=True,
        validation_data=(X_test_scaled, X_test_scaled),
        callbacks=[callback],
        verbose=1
    )

    # --- 5. SAVING MODEL ---
    autoencoder.save(MODEL_PATH)
    print(f"✅ Model saved at: {MODEL_PATH}")

    # Lưu lại danh sách cột training để lúc predict đảm bảo đúng thứ tự
    # (Mẹo nhỏ: lưu cái này để tránh lỗi lệch cột khi One-hot encoding)
    columns_path = os.path.join(os.path.dirname(MODEL_PATH), "model_columns.pkl")
    with open(columns_path, 'wb') as f:
        pickle.dump(X_train.columns.tolist(), f)
    print(f"ℹ️ Model columns info saved at: {columns_path}")

if __name__ == "__main__":
    train_model()