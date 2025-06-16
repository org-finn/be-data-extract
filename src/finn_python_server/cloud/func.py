import sys
import os
import io
import json
import logging
import asyncio
from fdk import response

from stock import stock_price_data
from news import news_data
from supabase import create_client, Client
from tiingo import TiingoClient


def handler(ctx, data: io.BytesIO=None):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()
    
    logger.info("=== 데이터 수집 파이프라인 시작 ===")

    try:
        # 1. 환경 변수 및 클라이언트 초기화
        tiingo_api_key = os.environ.get('TIINGO_API_KEY')
        supabase_url = os.environ.get('SUPABASE_URL')
        supabase_api_key = os.environ.get('SUPABASE_KEY')

        if not all([supabase_url, supabase_api_key]):
            raise ValueError("Supabase 환경 변수가 설정되지 않았습니다.")

        supabase: Client = create_client(supabase_url, supabase_api_key)
        
        # 2. 공통으로 사용할 주식 정보 가져오기 (이 로직도 별도 모듈로 뺄 수 있습니다)
        response = supabase.table('stocks').select('id, stock_code, search_keyword').execute()
        all_stocks = response.data
        if not all_stocks:
            logger.warning("DB에 조회할 주식이 없어 함수를 종료합니다.")
            return response.Response(ctx, response_data=json.dumps({"status": "No stocks to process"}), headers={"Content-Type": "application/json"})

        # 3. 주가 데이터 수집 모듈 실행
        if tiingo_api_key:
            tiingo_client = TiingoClient({'session': True, 'api_key': tiingo_api_key})
            if not stock_price_data._check_is_today_closed_day(tiingo_client):
                stock_price_data.collect_and_save_stock_prices(tiingo_client, supabase, all_stocks, logger)
            else:
                logger.info("금일이 휴장일이여서 주가 데이터 수집을 건너뜁니다.")
        else:
            logger.warning("TIINGO_API_KEY가 설정되지 않아 주가 데이터 수집을 건너뜁니다.")

        # 4. 뉴스 데이터 수집 모듈 실행 (비동기)
        asyncio.run(news_data.collect_and_save_news_async(supabase, all_stocks, logger))

        logger.info("=== 모든 데이터 수집 파이프라인 성공적으로 완료 ===")
        return response.Response(
            ctx, response_data=json.dumps({"status": "Success"}),
            headers={"Content-Type": "application/json"}
        )

    except Exception as e:
        logger.error(f"파이프라인 실행 중 심각한 오류 발생: {e}", exc_info=True)
        return response.Response(
            ctx, response_data=json.dumps({"status": "Error", "message": str(e)}),
            headers={"Content-Type": "application/json"},
            status_code=500
        )