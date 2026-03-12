import duckdb
import pandas as pd
from pathlib import Path


db_path = Path('data\\data.db')
spy_holdings_url = "https://www.ssga.com/us/en/intermediary/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spy.xlsx"
table_name = 'sp500'


def get_db_connection(db_path):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def create_table(con, table_name):
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


def load_df_into_db(con, df, table_name, temp_view='temp_df'):
    con.register(temp_view, df)
    con.execute(f'''
        INSERT OR IGNORE INTO {table_name} (ticker, company)
        SELECT ticker, company
        FROM {temp_view}''')
    con.unregister(temp_view)
