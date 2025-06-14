import os
from tiingo import TiingoClient
import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta
from dotenv import load_dotenv
from tqdm import tqdm

def collect_and_save_stock_prices(tiingo_client, supabase, stocks, logger):
    """주가 데이터 수집부터 저장까지의 전체 과정을 실행하는 메인 함수"""
    logger.info("--- 주가 데이터 수집 작업 시작 ---")
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    prices_to_insert = _stock_price_data_from_tiingo(tiingo_client, stocks, start_date, end_date, logger)
    if prices_to_insert:
        _save_stock_prices_in_db(prices_to_insert, supabase, logger)
    
    logger.info("--- 주가 데이터 수집 작업 완료 ---")

def _stock_price_data_from_tiingo(tiingo_client, stocks, start_date, end_date, logger):
    # (이전과 동일한 로직, 함수 이름 앞에 _를 붙여 내부용임을 표시)
    all_prices_to_insert = []
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    logger.info(f"{len(stocks)}개 주식에 대한 주가 데이터 수집 (기간: {start_date_str} ~ {end_date_str})")
    for stock in stocks:
        stock_id = stock['id']
        stock_code = stock.get('stock_code')
        if not stock_code: 
            continue
        try:
            price_df = tiingo_client.get_dataframe(stock_code, startDate=start_date_str, endDate=end_date_str, frequency='daily')
            if price_df.empty: 
                continue
            
            price_df.reset_index(inplace=True)
            price_df['stock_id'] = stock_id
            price_df['change_rate'] = 0.0
            price_df.rename(columns={'date': 'price_date', 'adjOpen': 'open_price', 
                                     'adjHigh': 'high_price', 'adjLow': 'low_price', 'close': 'close_price', 
                                     'adjClose' : 'adj_close_price'}, inplace=True)
            
            numeric_columns = ['change_rate', 'open_price', 'high_price', 'low_price', 'close_price', 'adj_close_price']
            for col in numeric_columns: 
                price_df[col] = pd.to_numeric(price_df[col], errors='coerce').round(4)
            price_df['price_date'] = pd.to_datetime(price_df['price_date']).dt.strftime('%Y-%m-%d')
            
            required_columns = ['stock_id', 'price_date', 'open_price', 'high_price', 'low_price', 'close_price', 
                                'adj_close_price', 'change_rate', 'volume']
            processed_df = price_df[required_columns].dropna()
            
            all_prices_to_insert.extend(processed_df.to_dict(orient='records'))
        except Exception as e:
            logger.error(f"'{stock_code}' 주가 처리 중 오류: {e}")
            continue
    logger.info(f"총 {len(all_prices_to_insert)}개의 주가 레코드를 처리했습니다.")
    return all_prices_to_insert

def _save_stock_prices_in_db(all_prices_to_insert, supabase, logger):
    # (이전과 동일한 로직, 함수 이름 앞에 _를 붙여 내부용임을 표시)
    try:
        response = supabase.table('stock_prices').upsert(all_prices_to_insert, on_conflict='stock_id, price_date').execute()
        if not response.data: 
            raise Exception("Supabase에 데이터가 저장되지 않음 (RLS 정책 등 확인)")
        logger.info(f"주가 저장 성공: {len(response.data)}개 레코드 처리")
    except Exception as e:
        logger.error(f"주가 저장 중 오류: {e}")
    
    