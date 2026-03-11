import pandas as pd
import data_preparation
from simple_market_analysis.return_after_jump.utils import identify_pos_jumps, get_jump_days, plot_hist, jump_analysis

ticker = 'SPY'
data_source = 'D:\\Jiheng\\Python Projects\\PythonProject\\simple_market_analysis\\return_after_jump\\data\\' + ticker + '.csv'

raw_data = pd.read_csv(data_source)
cleaned_data = data_preparation.clean_data(raw_data)
data_with_vol = data_preparation.prepare_variables(cleaned_data)
data_with_jump = identify_pos_jumps(data_with_vol)
jump_days = get_jump_days(data_with_jump)
plot_hist(jump_days['Return'])