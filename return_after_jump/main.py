import pandas as pd
import numpy as np
import data_preparation

ticker = 'SPY'
data_source = 'D:\\Jiheng\\Python Projects\\PythonProject\\simple_market_analysis\\return_after_jump\\data\\' + ticker + '.csv'

raw_data = pd.read_csv(data_source)
cleaned_data = data_preparation.clean_data(raw_data)
data_with_vol = data_preparation.prepare_variables(cleaned_data)

def identify_pos_jumps(data):
    data['Jump'] = (data['Return'] > 2 * data['Vol']).astype(int)
    return data


