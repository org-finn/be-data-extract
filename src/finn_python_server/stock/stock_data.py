import keyring
from tiingo import TiingoClient
import pandas as pd
from pandas import DataFrame as df

def get_stock_datas():
    api_key = keyring.get_password('System', 'anaconda77_tiingo')
    keyring.set_password('tiingo', 'anaconda77_tiingo', api_key)

    config = {}
    config['session'] = True
    config['api_key'] = api_key
    client = TiingoClient(config)


    # tesla_ticker_metadata = client.get_ticker_metadata('TSLA')
    # print(ticker_metadata)
    
    # 핵심 부분: 주가 데이터 받아오는 코드
    tsla_prices = client.get_dataframe('TSLA',
                                     startDate='2024-01-01',
                                     endDate='2024-12-31',
                                     frequency='daily')
    
    # column명 없이 받아오려면 뒤에 header=False 추가
    tsla_prices.to_csv('~/Downloads/tsla_prices_train.csv')

get_stock_datas()
    
    