import yfinance as yf

duration = '20y'
data_path = 'D:\\Jiheng\\Python Projects\\PythonProject\\simple_market_analysis\\return_after_jump\data'

def export_data_as_csv(ticker, duration):
    data = yf.download(ticker, period=duration, interval='1d')
    data.to_csv(data_path + '\\' + ticker + '.csv')


ticker = 'SPY'
export_data_as_csv(ticker, duration)