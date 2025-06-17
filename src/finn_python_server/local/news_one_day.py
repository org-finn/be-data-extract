import sys, os
from tiingo import TiingoClient
from supabase import create_client, Client
from dotenv import load_dotenv
import logging
from fdk import response
import asyncio

base_dir = os.path.dirname(__file__)
parent_path = os.path.join(base_dir, '..')
sys.path.append(parent_path)

from cloud.stock import stock_price_data
from cloud.news import news_data

async def main():
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

     # 4. 뉴스 데이터 수집 모듈 실행 (비동기)
    await news_data.collect_and_save_news_async(supabase, all_stocks, logger)

    logger.info("=== 모든 데이터 수집 파이프라인 성공적으로 완료 ===")
    return response.Response(
        ctx, response_data=json.dumps({"status": "Success"}),
        headers={"Content-Type": "application/json"}
    )

asyncio.run(main())