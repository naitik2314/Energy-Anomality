import numpy as np
import pandas as pd
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout, RepeatVector, TimeDistributed
from tensorflow.keras.optimizers import Adam
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import classification_report
from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint

# Parameters for synthetic data generation
num_meters = 250  # Number of meters
num_days = 5 * 365  # 5 years of data
sampling_rate = 15  # in minutes
anomaly_percentage = 0.25  # 25% anomalies

# Calculate total samples based on days and sampling rate
total_samples = num_days * 24 * (60 // sampling_rate)
time_index = pd.date_range(start="2010-01-01", periods=total_samples, freq=f"{sampling_rate}T")

# Initialize DataFrame with time index
data = pd.DataFrame(index=time_index)

# Generate baseline consumption patterns
for meter_id in range(1, num_meters + 1):
    daily_pattern = 0.5 + 0.2 * np.sin(np.linspace(0, 2 * np.pi, 24 * (60 // sampling_rate))) * (1 + np.random.normal(0, 0.01))
    weekly_pattern = 0.5 + 0.1 * np.sin(np.linspace(0, 2 * np.pi, 7 * 24 * (60 // sampling_rate))) * (1 + np.random.normal(0, 0.01))
    
    daily_pattern_repeated = np.tile(daily_pattern, num_days)
    weekly_pattern_repeated = np.tile(weekly_pattern, num_days // 7 + 1)[:total_samples]
    
    baseline_consumption = daily_pattern_repeated + weekly_pattern_repeated[:total_samples] + np.random.normal(0, 0.05, total_samples)
    data[f'meter_{meter_id}'] = baseline_consumption

# Function to inject anomalies
def inject_anomalies(data, anomaly_percentage, spike_factor=3, dip_factor=0.3, prolonged_anomaly_duration=4):
    num_anomalies = int(len(data) * anomaly_percentage)
    anomaly_indices = np.random.choice(data.index, size=num_anomalies, replace=False)
    
    data['anomaly'] = 0  # Column to track anomalies
    
    for idx in anomaly_indices:
        col = np.random.choice(data.columns[:-1])  # Random meter column
        
        if np.random.rand() > 0.5:
            data.loc[idx, col] *= spike_factor  # Sudden spike
        else:
            data.loc[idx, col] *= dip_factor  # Sudden dip
            
        # Prolonged anomalies
        for i in range(prolonged_anomaly_duration):
            future_idx = idx + pd.Timedelta(minutes=sampling_rate * i)
            if future_idx in data.index:
                data.loc[future_idx, col] *= spike_factor if np.random.rand() > 0.5 else dip_factor
                data.loc[future_idx, 'anomaly'] = 1

    return data

# Apply anomaly injection
data_with_anomalies = inject_anomalies(data.copy(), anomaly_percentage)

# Data Scaling
scaler = MinMaxScaler()
scaled_data = scaler.fit_transform(data_with_anomalies.drop(columns=['anomaly']))

# Convert data into supervised learning format for LSTM
def create_sequences(data, seq_length=48):
    X, y = [], []
    for i in range(len(data) - seq_length):
        X.append(data[i:i + seq_length])
        y.append(data_with_anomalies['anomaly'].iloc[i + seq_length])  # Label is the next point
    return np.array(X), np.array(y)

seq_length = 48  # 12 hours of 15-minute intervals
X, y = create_sequences(scaled_data, seq_length=seq_length)

# Split into train, validation, and test sets
train_size = int(0.7 * len(X))
val_size = int(0.15 * len(X))

X_train, y_train = X[:train_size], y[:train_size]
X_val, y_val = X[train_size:train_size + val_size], y[train_size:train_size + val_size]
X_test, y_test = X[train_size + val_size:], y[train_size + val_size:]

# LSTM Autoencoder Model
model = Sequential([
    LSTM(64, activation='relu', input_shape=(seq_length, X.shape[2]), return_sequences=True),
    Dropout(0.2),
    LSTM(32, activation='relu', return_sequences=False),
    RepeatVector(seq_length),
    LSTM(32, activation='relu', return_sequences=True),
    Dropout(0.2),
    LSTM(64, activation='relu', return_sequences=True),
    TimeDistributed(Dense(X.shape[2]))
])

model.compile(optimizer=Adam(learning_rate=0.001), loss='mae')

# Early stopping and model checkpointing
early_stopping = EarlyStopping(
    monitor='val_loss', 
    patience=5, 
    restore_best_weights=True
)

checkpoint = ModelCheckpoint(
    filepath='best_model.h5', 
    monitor='val_loss', 
    save_best_only=True, 
    verbose=1
)

# Model training with callbacks
history = model.fit(
    X_train, X_train,
    epochs=50, 
    batch_size=128,
    validation_data=(X_val, X_val),
    callbacks=[early_stopping, checkpoint]
)

# Set threshold for anomaly detection based on training loss
X_train_pred = model.predict(X_train)
train_mae_loss = np.mean(np.abs(X_train_pred - X_train), axis=(1, 2))
threshold = np.percentile(train_mae_loss, 95)

# Evaluate on test set
X_test_pred = model.predict(X_test)
test_mae_loss = np.mean(np.abs(X_test_pred - X_test), axis=(1, 2))
y_test_pred = (test_mae_loss > threshold).astype(int)

# Classification report
print("Test Set Classification Report:")
print(classification_report(y_test, y_test_pred))
