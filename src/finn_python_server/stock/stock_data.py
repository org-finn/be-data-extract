import os
from tiingo import TiingoClient
import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta
from dotenv import load_dotenv

def get_stock_from_db(supabase):
    try:
        response = supabase.table('stocks').select('id, stock_code').execute()
        stocks_to_fetch = response.data

        if not stocks_to_fetch:
            raise ValueError("'stocks' 테이블에 조회할 주식이 없습니다. 먼저 주식 정보를 등록해주세요.")
    except Exception as e:
        error_message = f"데이터 조회 중단: {e}"
        print(error_message) # CloudWatch 로그에 기록하기 위함
    
    return stocks_to_fetch

def stock_price_data_from_tiingo(tiingo_client, stocks_to_fetch, start_date, end_date):
    all_prices_to_insert = []
    
    start_date = start_date.strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')
    
    # 핵심 부분: 주가 데이터 받아오는 코드
    for stock in stocks_to_fetch:
        stock_id = stock['id']
        stock_code = stock['stock_code']
        
        try:
            price_df = tiingo_client.get_dataframe(
                stock_code,
                startDate=start_date,
                endDate=end_date,
                frequency='daily'
            )

            if price_df.empty:
                print(f"'{stock_code}'에 대한 데이터를 가져오지 못했습니다. 건너뜁니다.")
                continue
            
            price_df.reset_index(inplace=True) # date가 인덱스일경우, column으로 변경
            
            price_df['stock_id'] = stock_id
            price_df['change_rate'] = 0.0 # TODO: 이전 날짜 비교해서 등락률 구하는 로직 구현 예정
            
            price_df.rename(columns={
                'date': 'price_date',
                'adjOpen': 'open_price',
                'adjHigh': 'high_price',
                'adjLow': 'low_price',
                'adjClose': 'close_price'
                # 'volume'은 이름이 동일하므로 변경 필요 없음
            }, inplace=True)
            
            # NUMERIC(7, 4) 제약 조건에 맞게 가격 관련 컬럼들을 소수점 4자리로 반올림합니다.
            numeric_columns = ['change_rate', 'open_price', 'high_price', 'low_price', 'close_price']
            for col in numeric_columns:
                price_df[col] = price_df[col].round(4)
            
            price_df['price_date'] = pd.to_datetime(price_df['price_date']).dt.strftime('%Y-%m-%d')
            required_columns = ['stock_id', 'price_date', 'open_price', 'high_price', 'low_price', 'close_price', 'change_rate', 'volume']
            processed_df = price_df[required_columns]

            # DataFrame을 삽입 가능한 dict 리스트로 변환하여 전체 리스트에 추가
            all_prices_to_insert.extend(processed_df.to_dict(orient='records'))
        
        except Exception as e:
            print(f"'{stock_code}' 처리 중 오류 발생: {e}")
            continue
    
    return all_prices_to_insert

def save_stock_prices_in_db(all_prices_to_insert, supabase):
        
    try:
        # upsert: (stock_id, price_date)가 중복되면 업데이트, 없으면 삽입
        response = (
            supabase.table('stock_prices')
            .upsert(all_prices_to_insert, 
                on_conflict='stock_id, price_date',  # 중복 검사 기준이 되는 복합키
            )
            .execute()
        )

        # 실제 데이터가 처리되었는지 확인
        # RLS 정책 등에 의해 아무 작업도 수행되지 않았지만 에러는 없는 경우를 대비
        if not response.data:
            raise Exception("Supabase에 데이터가 저장되지 않았지만, 명시적인 에러는 없습니다. RLS 정책 등을 확인하세요.")
        
        # 모든 확인을 통과하면 성공으로 간주
        success_count = len(response.data)
        print(f"Supabase 저장 성공: {success_count}개 레코드 처리")
    except Exception as e:
        print(f"Supabase 저장 중 오류 발생: {e}")
    

def main():
    load_dotenv()
    
    tiingo_api_key = os.environ.get('TIINGO_API_KEY')
    supabase_url = os.environ.get('SUPABASE_URL')
    supabase_api_key = os.environ.get('SUPABASE_KEY')

    tiingo_client = TiingoClient({'session': True, 'api_key': tiingo_api_key})
    supabase: Client = create_client(supabase_url, supabase_api_key)
    
    stocks_to_fetch = get_stock_from_db(supabase)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    
    if stocks_to_fetch:
        all_prices_to_insert = stock_price_data_from_tiingo(tiingo_client, stocks_to_fetch, start_date, end_date)

        if all_prices_to_insert:
            save_stock_prices_in_db(all_prices_to_insert, supabase)

main()
    
    