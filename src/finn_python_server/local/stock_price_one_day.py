import sys, os
from tiingo import TiingoClient
from supabase import create_client, Client
from dotenv import load_dotenv
import logging
from fdk import response

base_dir = os.path.dirname(__file__)
parent_path = os.path.join(base_dir, '..')
sys.path.append(parent_path)

from cloud.stock import stock_price_data
from cloud.news import news_data

def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()
    load_dotenv()
    
    tiingo_api_key = os.environ.get('TIINGO_API_KEY')
    supabase_url = os.environ.get('SUPABASE_URL')
    supabase_api_key = os.environ.get('SUPABASE_KEY')

    if not all([supabase_url, supabase_api_key]):
        raise ValueError("Supabase 환경 변수가 설정되지 않았습니다.")

    supabase: Client = create_client(supabase_url, supabase_api_key)
    
    stocks_response = supabase.table('stocks').select('id, stock_code, search_keyword').execute()
    all_stocks = stocks_response.data
    if not all_stocks:
        print("DB에 조회할 주식이 없어 함수를 종료합니다.")
        return response.Response(ctx, response_data=json.dumps({"status": "No stocks to process"}), headers={"Content-Type": "application/json"})

    # 3. 주가 데이터 수집 모듈 실행
    if tiingo_api_key:
        tiingo_client = TiingoClient({'session': True, 'api_key': tiingo_api_key})
        if not stock_price_data.check_is_today_closed_day(tiingo_client, logger):
                stock_price_data.collect_and_save_stock_prices(tiingo_client, supabase, all_stocks, logger)
        else:
            print("금일이 휴장일이여서 주가 데이터 수집을 건너뜁니다.")
    else:
        print("TIINGO_API_KEY가 설정되지 않아 주가 데이터 수집을 건너뜁니다.")

main()