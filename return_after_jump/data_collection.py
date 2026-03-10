import yfinance as yf


data_path = 'D:\\Jiheng\\Python Projects\\PythonProject\\simple_market_analysis\\return_after_jump\data'

def export_data_as_csv(ticker):
    data = yf.download(ticker, period='5y', interval='1d')
    data.to_csv(data_path + '\\' + ticker + '.csv')


ticker = 'SPY'
export_data_as_csv(ticker)