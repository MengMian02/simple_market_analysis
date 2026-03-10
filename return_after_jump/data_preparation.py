import pandas as pd

ticker = 'SPY'

raw_data_path = 'D:\\Jiheng\\Python Projects\\PythonProject\\simple_market_analysis\\return_after_jump\data' + '\\' + ticker + '.csv'
raw_data = pd.read_csv(raw_data_path)
vol_window = 256
vol_min_period = 128


def clean_data(raw_data):
    data = raw_data.iloc[2:]
    data.rename(columns={'Price':'Date'}, inplace = True)
    data.set_index('Date', inplace = True)
    return data


def calc_past_vol(clean_data, vol_window = 256, vol_min_period = 128):
    clean_data['Vol'] = clean_data['Close'].rolling(window=vol_window, min_periods=vol_min_period).std()
    return clean_data

