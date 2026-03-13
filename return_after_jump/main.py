from pathlib import Path
from simple_market_analysis.return_after_jump.utils import (
    get_db_connection,
    analyse_group, conduct_hypothesis_test
)


t = 1
db_path = Path('data\\data.db')
jump_dir = 'positive'
param_of_interest = 'perc_positive'


if __name__ == '__main__':
    con = get_db_connection(db_path)
    sp_dict = analyse_group(con, 'sp500', 'sp_prices_daily', jump_dir, param_of_interest, t)
    micro_dict = analyse_group(con, 'micro', 'micro_prices_daily', jump_dir, param_of_interest, t)
    conduct_hypothesis_test(sp_dict, micro_dict)

# plot_hist(jump_days['Return'])



