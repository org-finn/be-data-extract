import keyring
import finnhub
import pandas as pd
from pandas import DataFrame as df


def get_news_data():
    api_key = keyring.get_password('System', 'anaconda77_finnhub')
    
    finnhub_client = finnhub.Client(api_key=api_key)
    
    tsla_newsletters = finnhub_client.company_news('TSLA', _from="2025-01-01", to="2025-05-09")
    tsla_df = df.from_records(tsla_newsletters)
    print(tsla_df)
    tsla_df.to_csv('~/Downloads/tesla_news.csv')
    

