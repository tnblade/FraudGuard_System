# train_model.py
import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Input, Dense
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import pickle
import glob
from configs import MODEL_PATH, SCALER_PATH

def train():
    print("🔄 Loading Dataset for Training...")
    # Tìm file csv trong input Kaggle
    files = glob.glob("/kaggle/input/**/*.csv", recursive=True)
    if not files:
        print("❌ Không tìm thấy dataset Paysim!")
        return
    
    df = pd.read_csv(files[0])
    
    # Preprocessing nhanh
    df = pd.get_dummies(df[['type', 'amount', 'oldbalanceOrg', 'newbalanceOrig', 'oldbalanceDest', 'newbalanceDest', 'isFraud']], columns=['type'], prefix='type')
    df = df.astype(float)
    
    # Chỉ train trên data sạch
    X_normal = df[df['isFraud'] == 0].drop(['isFraud'], axis=1)
    X_train, _ = train_test_split(X_normal, test_size=0.2, random_state=42)
    
    # Scaling
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    
    # Lưu scaler
    with open(SCALER_PATH, 'wb') as f:
        pickle.dump(scaler, f)
        
    # Build Model Autoencoder
    input_dim = X_train_scaled.shape[1]
    input_layer = Input(shape=(input_dim,))
    encoder = Dense(8, activation="tanh")(input_layer)
    latent = Dense(4, activation="tanh")(encoder)
    decoder = Dense(8, activation="tanh")(latent)
    output_layer = Dense(input_dim, activation="linear")(decoder)
    
    autoencoder = Model(inputs=input_layer, outputs=output_layer)
    autoencoder.compile(optimizer='adam', loss='mean_squared_error')
    
    print("🏋️ Training Model...")
    autoencoder.fit(X_train_scaled, X_train_scaled, epochs=5, batch_size=2048, verbose=1)
    
    autoencoder.save(MODEL_PATH)
    print(f"✅ Model saved to {MODEL_PATH}")

if __name__ == "__main__":
    train()