import duckdb
import numpy as np
from matplotlib import pyplot as plt
from scipy import stats

from simple_market_analysis.return_after_jump import data_preparation


def identify_jumps(data, dir):
    if dir == 'positive':
        data['Jump'] = (data['Return'] > 2 * data['Vol']).astype(int)
    elif dir == 'negative':
        data['Jump'] = (data['Return'] < -2 * data['Vol']).astype(int)
    return data


def get_post_jump_days(data, t):
    jump_days = data[data['Jump'].shift(t) == 1]
    return jump_days


def jump_analysis(data):
    mean = data['Return'].mean()
    median = data['Return'].median()
    max_return = data['Return'].max()
    min_return = data['Return'].min()
    perc_pos = (data['Return'] > 0).mean() * 100
    summary = {
        'mean': mean,
        'median': median,
        'max_return': max_return,
        'min_return': min_return,
        'perc_positive': perc_pos,
    }
    return summary


def plot_hist(data, bin_number=10):
    plt.hist(data, bins=bin_number)
    plt.show()


def get_db_connection(db_path):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def get_tickers(con, ticker_table):
    rows = con.execute(f"""
        SELECT ticker FROM {ticker_table}
        ORDER BY ticker""").fetchall()
    return [row[0] for row in rows]


def get_price_from_db(ticker, con, price_table):
    data = con.execute(f"""
        SELECT * FROM {price_table}
        WHERE ticker = '{ticker}'""").df()
    return data


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
