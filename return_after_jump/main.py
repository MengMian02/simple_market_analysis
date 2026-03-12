import pandas as pd
import data_preparation
from simple_market_analysis.return_after_jump.utils import identify_jumps, get_post_jump_days, plot_hist, jump_analysis

t = 1


def main(ticker, dir, t):
    # data_source = 'D:\\Jiheng\\Python Projects\\PythonProject\\simple_market_analysis\\return_after_jump\\data\\' + ticker + '.csv'
    # raw_data = pd.read_csv(data_source)
    # cleaned_data = data_preparation.clean_data(raw_data)
    data_with_vol = data_preparation.prepare_variables(cleaned_data)
    data_with_jump = identify_jumps(data_with_vol, dir)
    jump_days = get_post_jump_days(data_with_jump, t)
    summary = jump_analysis(jump_days)
    return summary


# plot_hist(jump_days['Return'])



