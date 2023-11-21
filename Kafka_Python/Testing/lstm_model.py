import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from datetime import datetime
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
from sklearn.metrics import mean_squared_error
import matplotlib.pyplot as plt


def lstm(st_data): 
    preprocessed_df = preprocess(st_data)
    print(preprocessed_df)
    dates,seq,tar = create_sequences(st_data, 5)
    print(dates, seq, tar)

    split = int(len(seq)*0.7)
    dates_train, dates_test, X_train, X_test, y_train, y_test = dates[:split], dates[split:], seq[:split], seq[split:], tar[:split], tar[split:]

    model = Sequential()
    model.add(LSTM(50, activation = 'relu', input_shape = (5, 1)))
    model.add(Dense(1))
    model.compile(optimizer='adam', loss='mean_squared_error')

    X_train = X_train.reshape((X_train.shape[0], X_train.shape[1], 1))
    X_test = X_test.reshape((X_test.shape[0], X_test.shape[1], 1))
    print(y_train)

    history = model.fit(X_train, y_train, epochs=20, batch_size=32, validation_data=(X_test, y_test))    

    y_pred = model.predict(X_test)

    return y_pred


def preprocess(st_data):
    # normalizing the data
    scaler = MinMaxScaler(feature_range=(0, 1))
    normalized_data = scaler.fit_transform(st_data)
    print(st_data)
    
    #store dates in a separate column
    st_data['Date'] = st_data.index
    st_data.index = range(1, len(st_data)+1)
    st_data

    st_data = st_data[['Date','Close']]
    #just keep the date
    st_data['Date'] = st_data['Date'].apply(lambda x: x.timestamp())

def create_sequences(stock_data, length):
  sequences = []
  target_prices = []
  dates = []

  for i in range(len(stock_data) - length):
      seq = stock_data['Close'][i:i+length]
      date = stock_data['Date'][i+length]
      target_price = stock_data['Close'][i+length]
      dates.append(date)
      sequences.append(seq)
      target_prices.append(target_price)

  return np.array(dates), np.array(sequences), np.array(target_prices)
