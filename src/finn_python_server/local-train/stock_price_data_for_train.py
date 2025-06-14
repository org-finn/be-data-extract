import os
from tiingo import TiingoClient
import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta
from dotenv import load_dotenv
from tqdm import tqdm
from config import STOCK_LIST
    

def get_stock_price_for_train(tiingo_client, stock_code):
    start_date = '2024-01-01'
    end_date = '2024-12-31'
    
    try:
        price_df = tiingo_client.get_dataframe(
            stock_code,
            startDate=start_date,
            endDate=end_date,
            frequency='daily'
        )

        price_df.to_csv(f'~/Downloads/finn_data/price/{stock_code}_prices_train.csv')
        
    except Exception as e:
            tqdm.write(f"'{stock_code}' 처리 중 오류 발생: {e}")


def get_stock_price_for_test(tiingo_client, stock_code):
    start_date = '2025-01-01'
    end_date = '2025-05-31'
    
    try:
        price_df = tiingo_client.get_dataframe(
            stock_code,
            startDate=start_date,
            endDate=end_date,
            frequency='daily'
        )

        price_df.to_csv(f'~/Downloads/finn_data/price/{stock_code}_prices_test.csv')
        
    except Exception as e:
            tqdm.write(f"'{stock_code}' 처리 중 오류 발생: {e}")


def main():
    load_dotenv()
    
    tiingo_api_key = os.environ.get('TIINGO_API_KEY')
    tiingo_client = TiingoClient({'session': True, 'api_key': tiingo_api_key})
    
    # --- 학습용(Train) 데이터 다운로드 ---
    print("학습용 데이터 다운로드를 시작합니다.")
    for stock_code in tqdm(STOCK_LIST, desc="Fetching Train Data"):
        get_stock_price_for_train(tiingo_client, stock_code)
    
    print("\n학습용 데이터 다운로드 완료.")
    
    # --- 테스트용(Test) 데이터 다운로드 ---
    print("\n테스트용 데이터 다운로드를 시작합니다.")
    for stock_code in tqdm(STOCK_LIST, desc="Fetching Test Data"):
        get_stock_price_for_test(tiingo_client, stock_code)

    print("\n모든 데이터 다운로드가 완료되었습니다.")
    

main()
    
    