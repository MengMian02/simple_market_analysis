import data_preparation
from pathlib import Path
from scipy import stats
import numpy as np
from simple_market_analysis.return_after_jump.utils import (
    identify_jumps,
    get_post_jump_days,
    jump_analysis,
    get_db_connection,
    get_tickers, get_price_from_db
)


t = 1
db_path = Path('data\\data.db')
jump_dir = 'positive'
param_of_interest = 'perc_positive'
con = get_db_connection(db_path)


def analyse_group(con, ticker_table, price_table, jump_dir, param_of_interest, t):
    unfiltered_tickers = get_tickers(con, ticker_table)
    tickers = [ticker for ticker in unfiltered_tickers if isinstance(ticker, str) and all(c.isupper() for c in ticker)]
    dict = {}
    for ticker in tickers:
        raw_data = get_price_from_db(ticker, con, price_table).sort_values('date', ascending=True)
        data_with_vol = data_preparation.prepare_variables(raw_data)
        data_with_jump = identify_jumps(data_with_vol, jump_dir)
        jump_days = get_post_jump_days(data_with_jump, t)
        summary = jump_analysis(jump_days)
        dict[ticker] = summary[param_of_interest]
    return dict


def conduct_hypothesis_test(dict1, dict2):
    list1 = np.array(list(dict1.values()))
    list2 = np.array(list(dict2.values()))
    list1 = list1[~np.isnan(list1)]
    list2 = list2[~np.isnan(list2)]
    t_stat, p_value = stats.ttest_ind(list1, list2, equal_var=False)
    print("Mean of list1:", list1.mean())
    print("Mean of list2:", list2.mean())
    print("t-statistic:", t_stat)
    print("p-value:", p_value)


sp_dict = analyse_group(con, 'sp500', 'sp_prices_daily', jump_dir, param_of_interest, t)
micro_dict = analyse_group(con, 'micro', 'micro_prices_daily', jump_dir, param_of_interest, t)
conduct_hypothesis_test(sp_dict, micro_dict)

# plot_hist(jump_days['Return'])



