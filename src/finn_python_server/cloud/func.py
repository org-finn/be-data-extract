import os
import io
import json
import logging
import oci
from fdk import response

from stock import stock_price_data
from news import news_data
from supabase import create_client, Client
from tiingo import TiingoClient
import exceptions
import queue_manager
from datetime import datetime

async def handler(ctx, data: io.BytesIO=None):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()
    
    logger.info("=== 데이터 수집 파이프라인 시작 ===")

    try:
        # 1. 환경 변수 및 클라이언트 초기화
        tiingo_api_key = os.environ.get('TIINGO_API_KEY')
        supabase_url = os.environ.get('SUPABASE_URL')
        supabase_api_key = os.environ.get('SUPABASE_KEY')

        if not all([supabase_url, supabase_api_key]):
            raise exceptions.ConfigError("Supabase 환경 변수가 설정되지 않았습니다.")

        supabase: Client = create_client(supabase_url, supabase_api_key)
        
        # 2. 공통으로 사용할 주식 정보 가져오기 (이 로직도 별도 모듈로 뺄 수 있습니다)
        stocks_response = supabase.table('stocks').select('id, stock_code, search_keyword').execute()
        if not stocks_response.data and stocks_response.data is not None: # data가 있고 비어있는 경우는 정상이지만, 에러로 data 자체가 없을 수 있음
             pass # 정상 케이스
        elif not hasattr(stocks_response, 'data'):
             raise exceptions.SupabaseError("주식 목록 조회 실패: Supabase 응답에 'data' 필드가 없습니다.")
        
        all_stocks = stocks_response.data
        if not all_stocks:
            logger.warning("DB에 조회할 주식이 없어 함수를 종료합니다.")
            return response.Response(ctx, response_data=json.dumps({"status": "No stocks to process"}), headers={"Content-Type": "application/json"})

        # 3. 주가 데이터 수집 모듈 실행
        if tiingo_api_key:
            tiingo_client = TiingoClient({'session': True, 'api_key': tiingo_api_key})
            if not stock_price_data.check_is_today_closed_day(tiingo_client, logger):
                stock_price_data.collect_and_save_stock_prices(tiingo_client, supabase, all_stocks, logger)
            else:
                logger.info("금일이 휴장일이여서 주가 데이터 수집을 건너뜁니다.")
        else:
            logger.warning("TIINGO_API_KEY가 설정되지 않아 주가 데이터 수집을 건너뜁니다.")

        # 4. 뉴스 데이터 수집 모듈 실행 (비동기)
        await news_data.collect_and_save_news_async(supabase, all_stocks, logger)

        # 5. 작업 완료 및 메시징 큐에 메시지 삽입
        
        # 모든 작업이 성공적으로 끝난 후, 큐 모듈을 호출하여 메시지를 보냅니다.
        logger.info("모든 데이터 수집 완료. 큐에 완료 메시지를 보냅니다.")
        
        # queue_manager 모듈의 함수를 호출
        queue_manager.send_completion_message(logger)
        
        logger.info("=== 모든 데이터 수집 파이프라인 성공적으로 완료 ===")
        return response.Response(
            ctx, response_data=json.dumps({
                "created_date" : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "status" : "Success",
                "message" : "주가/뉴스 데이터 수집을 정상적으로 수행하였습니다."
            }),
            headers={"Content-Type": "application/json"}
        )

    except exceptions.ConfigError as e:
        logger.critical(f"설정 오류 발생: {e}", exc_info=True)
        return response.Response(
            ctx, response_data=json.dumps({"status": "Config Error", "message": str(e)}),
            headers={"Content-Type": "application/json"}, status_code=500
        )
    except exceptions.ApiError as e:
        logger.error(f"외부 API 오류 발생: {e}", exc_info=True)
        return response.Response(
            ctx, response_data=json.dumps({"status": "API Error", "message": str(e)}),
            headers={"Content-Type": "application/json"}, status_code=503 # Service Unavailable
        )
    except exceptions.DbError as e:
        logger.error(f"데이터베이스 오류 발생: {e}", exc_info=True)
        return response.Response(
            ctx, response_data=json.dumps({"status": "Database Error", "message": str(e)}),
            headers={"Content-Type": "application/json"}, status_code=503 # Service Unavailable
        )
    except Exception as e:
        # [수정] 이곳은 예측하지 못한 모든 예외를 잡는 최후의 보루입니다.
        logger.critical(f"파이프라인 실행 중 예측하지 못한 심각한 오류 발생: {e}", exc_info=True)
        return response.Response(
            ctx, response_data=json.dumps({"status": "Internal Server Error", "message": str(e)}),
            headers={"Content-Type": "application/json"}, status_code=500
        )