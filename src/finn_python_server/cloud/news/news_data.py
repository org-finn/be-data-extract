import feedparser
from datetime import datetime, timedelta
import pandas as pd
from supabase import create_client, Client
import asyncio 
import aiohttp 
import exceptions

pandas_ts = pd.Timestamp.now(tz='Asia/Seoul')

async def collect_and_save_news_async(supabase, stocks, logger):
    """뉴스 데이터 수집부터 저장까지의 전체 과정을 비동기적으로 실행하는 메인 함수"""
    logger.info("--- 뉴스 데이터 수집 작업 시작 ---")
    
    end_day = datetime.now()
    start_day = end_day
    
    all_news = await _get_news_data_async(stocks, start_day, end_day, logger)
    if all_news:
        _save_news_in_db(all_news, supabase, logger)
        
    logger.info("--- 뉴스 데이터 수집 작업 완료 ---")


async def _get_news_data_async(stocks, start_day, end_day, logger):
    # (이전과 동일한 로직, 함수 이름 앞에 _를 붙여 내부용임을 표시)
    tasks = []
    logger.info(f"{len(stocks)}개 주식에 대한 뉴스 동시 수집 시작...")
    async with aiohttp.ClientSession() as session:
        for stock in stocks:
            query = stock.get('search_keyword')
            stock_id = stock['id']
            if not query: continue
            total_days = (end_day - start_day).days + 1
            for i in range(total_days):
                current_day = start_day + timedelta(days=i)
                task = _fetch_news_rss_day_async(logger, session, query, stock_id, current_day)
                tasks.append(task)
        
        results = await asyncio.gather(*tasks)

    all_news = [item for sublist in results for item in sublist]
    logger.info(f"총 {len(all_news)}개의 뉴스 기사 수집 완료. 중복 제거 시작...")
    
    unique_news = _remove_duplicate_titles_by_prefix(all_news, prefix_length=50)
    logger.info(f"중복 제거 후 {len(unique_news)}개의 뉴스 기사 남음.")
    
    return unique_news

def _save_news_in_db(all_news, supabase, logger):
    # (이전과 동일한 로직, 함수 이름 앞에 _를 붙여 내부용임을 표시)
    try:
        response = supabase.table('news').insert(all_news).execute()
        if not response.data: 
            raise exceptions.SupabaseError("Supabase에 뉴스 데이터 저장 실패 (응답 데이터 없음). RLS 정책 등을 확인하세요.")
        logger.info(f"뉴스 저장 성공: {len(response.data)}개 레코드 처리")
    except Exception as e:
        logger.error(f"뉴스 저장 중 심각한 오류: {e}")
        raise exceptions.SupabaseError(f"뉴스 저장 중 DB 오류 발생: {e}") from e

# --- 뉴스 수집을 위한 나머지 헬퍼 함수들 ---
def _generate_google_rss_url(query, start_date, end_date):
    base_url = "https://news.google.com/rss/search?"
    q = f"q={query}+after:{start_date}+before:{end_date}"
    params = "&hl=en-US&gl=US&ceid=US:en"
    return base_url + q

def _adjust_title_by_length_limit(title):
    return (title[:97] + '...') if len(title) > 100 else title

async def _fetch_news_rss_day_async(logger, session, query, stock_id, day: datetime, limit: int = 30):
    start_date = day.strftime("%Y-%m-%d")
    end_date = (day + timedelta(days=1)).strftime("%Y-%m-%d")
    url = _generate_google_rss_url(query, start_date, end_date)
    items = []
    try:
        async with session.get(url, timeout=10) as response:
            if response.status != 200:
                logger.warning(f"뉴스 RSS 피드 요청 실패 (상태 코드: {response.status}, URL: {url})")
                return []
            feed_text = await response.text()
            feed = feedparser.parse(feed_text)
            for entry in feed.entries[:limit]:
                try: pub_date = datetime(*entry.published_parsed[:6]).strftime('%Y-%m-%dT%H:%M:%S%z')
                except Exception: continue
                items.append({"published_date": pub_date, "title": _adjust_title_by_length_limit(entry.title), 
                              "original_url": entry.link, "company_name" : query, "view_count" : 0, 
                              "like_count" : 0, "stock_id" : stock_id, "created_at" : pandas_ts.strftime('%Y-%m-%dT%H:%M:%S%z')})
    except Exception as e:
        logger.warning(f"뉴스 피드 파싱/처리 중 개별 오류 발생 (Query: {query}, Day: {day.strftime('%Y-%m-%d')}): {e}")
    return items

def _remove_duplicate_titles_by_prefix(all_news, prefix_length=50):
    seen = set()
    keep_rows = []
    for news in all_news:
        prefix = news["title"][:prefix_length].strip().lower()
        if prefix not in seen:
            seen.add(prefix)
            keep_rows.append(news)
    return keep_rows
