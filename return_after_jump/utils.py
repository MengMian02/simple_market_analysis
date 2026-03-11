from matplotlib import pyplot as plt


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
        'perc_pos': perc_pos,
    }
    return summary


def plot_hist(data, bin_number=10):
    plt.hist(data, bins=bin_number)
    plt.show()
