import pandas as pd


vol_window = 256
vol_min_period = 128


def clean_data(raw_data):
    data = raw_data.iloc[2:]
    data.rename(columns={'Price':'Date'}, inplace = True)
    data.set_index('Date', inplace = True)
    return data


def prepare_variables(clean_data, vol_window = 256, vol_min_period = 128):
    clean_data = clean_data.drop('ticker', axis=1)
    clean_data = clean_data.apply(pd.to_numeric, errors = 'coerce')
    clean_data['Return'] = clean_data['Close'].pct_change()
    clean_data['Vol'] = clean_data['Return'].rolling(window=vol_window, min_periods=vol_min_period).std()
    return clean_data.dropna()

