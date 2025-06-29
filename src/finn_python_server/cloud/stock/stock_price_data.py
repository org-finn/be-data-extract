import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta
import traceback

# import sys,os
# base_dir = os.path.dirname(__file__)
# parent_path = os.path.join(base_dir, '..')
# sys.path.append(parent_path)
import exceptions

pandas_ts = pd.Timestamp.now(tz='Asia/Seoul')

def collect_and_save_stock_prices(tiingo_client, supabase, stocks, logger):
    """주가 데이터 수집부터 저장까지의 전체 과정을 실행하는 메인 함수"""
    logger.info("--- 주가 데이터 수집 작업 시작 ---")
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)
    
    prices_to_insert = _stock_price_data_from_tiingo(tiingo_client, supabase, stocks, start_date, end_date, logger)
    if prices_to_insert:
        _save_stock_prices_in_db(prices_to_insert, supabase, logger)
    
    logger.info("--- 주가 데이터 수집 작업 완료 ---")

def _stock_price_data_from_tiingo(tiingo_client, supabase, stocks, start_date, end_date, logger):
    all_prices_to_insert = []
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    logger.info(f"{len(stocks)}개 주식에 대한 주가 데이터 수집 (기간: {start_date_str} ~ {end_date_str})")
    
    id_to_last_day_prices = _get_last_day_prices(supabase, logger)
    for stock in stocks:
        stock_id = stock['id']
        stock_code = stock.get('stock_code')
        if not stock_code: 
            continue
        try:
            price_df = tiingo_client.get_dataframe(stock_code, startDate=start_date_str, endDate=end_date_str, frequency='daily')
            if price_df.empty: 
                logger.warning(f"'{stock_code}'에 대한 Tiingo 데이터를 가져올 수 없습니다. 건너뜁니다.")
                continue # 다음 주식으로 넘어감

            price_df.reset_index(inplace=True)
            price_df['stock_id'] = stock_id
            if stock_id in id_to_last_day_prices:
               price_df['change_rate'] = _calculate_change_rate_for_close(price_df['close'], id_to_last_day_prices[stock_id])
            else:
                price_df['change_rate'] = 0.00
            price_df.rename(columns={'date': 'price_date', 'adjOpen': 'open_price', 
                                     'adjHigh': 'high_price', 'adjLow': 'low_price', 'close': 'close_price', 
                                     'adjClose' : 'adj_close_price'}, inplace=True)
            
            numeric_columns = ['change_rate', 'open_price', 'high_price', 'low_price', 'close_price', 'adj_close_price']
            for col in numeric_columns: 
                price_df[col] = pd.to_numeric(price_df[col], errors='coerce').round(4)
            price_df['price_date'] = pd.to_datetime(price_df['price_date']).dt.strftime('%Y-%m-%d')
            price_df['created_at'] = pandas_ts.strftime('%Y-%m-%dT%H:%M:%S%z')
            
            required_columns = ['stock_id', 'price_date', 'open_price', 'high_price', 'low_price', 'close_price', 
                                'adj_close_price', 'change_rate', 'volume', 'created_at']
            processed_df = price_df[required_columns].dropna()
            
            all_prices_to_insert.extend(processed_df.to_dict(orient='records'))
        except Exception as e:
            logger.error(f"'{stock_code}' 주가 처리 중 오류 발생. 건너뜁니다: {e}")
            traceback.print_exc() # 상세 스택 트레이스 확인을 위해 유지
            continue # 다음 주식으로 넘어감
        
    logger.info(f"총 {len(all_prices_to_insert)}개의 주가 레코드를 처리했습니다.")
    return all_prices_to_insert

def _save_stock_prices_in_db(all_prices_to_insert, supabase, logger):
    try:
        response = supabase.table('stock_prices').upsert(all_prices_to_insert, on_conflict='stock_id, price_date').execute()
        if not response.data: 
            raise exceptions.SupabaseError("Supabase에 주가 데이터 저장 실패 (응답 데이터 없음). RLS 정책 등을 확인하세요.")
        logger.info(f"주가 저장 성공: {len(response.data)}개 레코드 처리")
    except Exception as e:
        logger.error(f"주가 저장 중 오류: {e}")
        logger.error(f"주가 저장 중 심각한 오류: {e}")
        raise exceptions.SupabaseError(f"주가 저장 중 DB 오류 발생: {e}") from e

def _get_last_day_prices(supabase, logger):
    try:
        id_to_prices = {}
        latest_date_response = supabase.table('stock_prices') \
                .select('price_date') \
                .order('price_date', desc=True) \
                .limit(1) \
                .execute()
        if not latest_date_response.data:
            logger.warning("stock_prices 테이블에 데이터가 없어 전일 종가 조회를 건너뜁니다.")
            return {} # 빈 딕셔너리를 반환하여 뒷부분 로직이 정상적으로 처리되도록 함
                
        latest_date = latest_date_response.data[0]['price_date']
        last_day_price_response = supabase.table('stock_prices').select('close_price, stock_id') \
            .eq('price_date', latest_date) \
            .execute()
        
        # 여기서도 데이터가 없을 수 있으므로 확인
        if not last_day_price_response.data:
            logger.warning(f"{latest_date}에 해당하는 주가 데이터가 없습니다. 전일 종가 조회를 건너뜁니다.")
            return {}
            
        for row in last_day_price_response.data:
            stock_id = row['stock_id']
            close_price = row['close_price']
            id_to_prices[stock_id] = close_price
        
        return id_to_prices
    except Exception as e:
        logger.error(f"어제 자 주가 조회 중 오류: {e}")
        raise exceptions.SupabaseError(f"어제 자 주가 조회 중 DB 오류 발생: {e}") from e

def _calculate_change_rate_for_close(today_price, last_day_price):
    change_rate = ((today_price - last_day_price) / last_day_price) * 100
    return change_rate.round(2)


def check_is_today_closed_day(tiingo_client, logger):
    
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=1)
        price_df = tiingo_client.get_ticker_price('AAPL', fmt='json', startDate=start_date.strftime('%Y-%m-%d'),
                                            endDate=end_date.strftime('%Y-%m-%d'), frequency='daily')
        if not price_df:
            return True
        return False
    except Exception as e:
        logger.error(f"휴장일 확인 중 Tiingo API 오류 발생: {e}")
        raise exceptions.TiingoApiError(f"휴장일 확인 중 API 오류 발생: {e}") from e
    
    