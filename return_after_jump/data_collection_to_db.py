import duckdb
import pandas as pd
from pathlib import Path
import yfinance as yf
from numpy.linalg.lapack_lite import dgelsd

db_path = Path('data\\data.db')
spy_holdings_url = "https://www.ssga.com/us/en/intermediary/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spy.xlsx"
table_name = 'sp500'


def get_db_connection(db_path):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def create_ticker_table(con, table_name):
    con.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
    ticker TEXT PRIMARY KEY,
    company TEXT)""")


def fetch_spy_holdings(url):
    pd.read_excel(url, engine='openpyxl', skiprows=4)


def clean_constituent_table(df):
    df = validate_rename_col(df, {'Name':'company',
                              'Ticker':'ticker'})[['ticker', 'company']]
    df['ticker'] = df['ticker'].str.replace('.', '-', regex=False)
    return df


def validate_rename_col(df, mapping):
    missing = [col for col in mapping.keys() if col not in df.columns]

    if missing:
        raise ValueError(
            f'Missing required columns: {missing}\n'
            f'Available columns: {list(df.columns)}'
        )
    return df.rename(columns=mapping)


def load_firm_df_into_db(con, df, table_name, temp_view='temp_df'):
    con.register(temp_view, df)
    con.execute(f'''
        INSERT OR IGNORE INTO {table_name} (ticker, company)
        SELECT ticker, company
        FROM {temp_view}''')
    con.unregister(temp_view)


def create_price_table(con, table_name='prices_daily'):
    con.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        ticker TEXT,
        date DATE, 
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        PRIMARY KEY(ticker, date)
        """)


def get_tickers(con, ticker_table):
    rows = con.execute(f"""
        SELECT ticker FROM {ticker_table}
        ORDER BY ticker""").fetchall()
    return [row[0] for row in rows]


def get_price_history(ticker, size, period, interval='1d'):
    df = yf.download(ticker, period=period, interval=interval)
    df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]
    df = df.reset_index()
    df['ticker'] = ticker
    df['size'] = size
    return df


def load_price_df_into_db(con, df, table_name, temp_view='temp_prices'):
    con.register(temp_view, df)
    con.execute(f'''
        INSERT OR IGNORE INTO {table_name} (ticker, date, Close, High, Low, Open, Volume, size)
        SELECT ticker, date, Close, High, Low, Open, Volume, size
        FROM {temp_view}''')
    con.unregister(temp_view)