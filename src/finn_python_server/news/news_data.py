import os
import feedparser
from datetime import datetime, timedelta
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
from tqdm.asyncio import tqdm 
import asyncio 
import aiohttp 

def get_stock_from_db(supabase):
    try:
        response = supabase.table('stocks').select('id, search_keyword').execute()
        stocks_to_fetch = response.data

        if not stocks_to_fetch:
            raise ValueError("'stocks' 테이블에 조회할 주식이 없습니다. 먼저 주식 정보를 등록해주세요.")
    except Exception as e:
        error_message = f"데이터 조회 중단: {e}"
        print(error_message) # CloudWatch 로그에 기록하기 위함
    
    return stocks_to_fetch

def generate_google_rss_url(query, start_date, end_date):
    base_url = "https://news.google.com/rss/search?"
    q = f"q={query}+after:{start_date}+before:{end_date}"
    params = "&hl=en-US&gl=US&ceid=US:en"
    return base_url + q + params

def adjust_title_by_length_limit(title):
    return (title[:97] + '...') if len(title) > 100 else title

async def fetch_news_rss_day_async(session, query, stock_id, day: datetime, limit: int = 30):
    start_date = day.strftime("%Y-%m-%d")
    end_date = (day + timedelta(days=1)).strftime("%Y-%m-%d")
    url = generate_google_rss_url(query, start_date, end_date)
    
    items = []
    try:
        async with session.get(url, timeout=10) as response:
            if response.status != 200:
                return [] # 응답 실패 시 빈 리스트 반환
            
            feed_text = await response.text()
            feed = feedparser.parse(feed_text)

            for entry in feed.entries[:limit]:
                try:
                    pub_date = datetime(*entry.published_parsed[:6])
                except Exception:
                    continue
                items.append({
                    "created_date": pub_date.strftime("%Y-%m-%d"),
                    "title": adjust_title_by_length_limit(entry.title),
                    "original_url": entry.link,
                    "company_name" : query,
                    "view_count" : 0,
                    "like_count" : 0,
                    "stock_id" : stock_id
                })
    except Exception as e:
        # print(f"Error fetching {query} on {start_date}: {e}") # 디버깅 시 사용
        pass # 개별 요청 실패 시 무시하고 계속 진행

    return items

def remove_duplicate_titles_by_prefix(all_news, prefix_length=50):
    seen = set()
    keep_rows = []

    for news in all_news:
        prefix = news["title"][:prefix_length].strip().lower()
        if prefix not in seen:
            seen.add(prefix)
            keep_rows.append(news)

    return keep_rows

async def get_news_data_async(fetch_from_stocks, start_day, end_day):
    """[asyncio] 모든 뉴스 수집 작업을 비동기적으로 동시에 실행합니다."""
    tasks = []
    async with aiohttp.ClientSession() as session:
        for stock in fetch_from_stocks:
            query = stock['search_keyword']
            stock_id = stock['id']
            
            total_days = (end_day - start_day).days + 1
            for i in range(total_days):
                current_day = start_day + timedelta(days=i)
                # 각 날짜와 주식에 대한 비동기 작업을 생성하여 리스트에 추가
                task = fetch_news_rss_day_async(session, query, stock_id, current_day)
                tasks.append(task)
        
        # tqdm.gather를 사용하여 모든 작업을 동시에 실행하고 진행 상황을 표시
        results = await tqdm.gather(*tasks, desc="뉴스 동시 수집 중")

    # 결과는 리스트의 리스트 형태이므로, 하나의 리스트로 펼쳐줍니다.
    all_news = [item for sublist in results for item in sublist]
    
    print("중복 제거 작업 진행 중...")
    all_news = remove_duplicate_titles_by_prefix(all_news, prefix_length=50)
    
    return all_news

def save_news_in_db(all_news, supabase):
    try:
        # upsert: (stock_id, price_date)가 중복되면 업데이트, 없으면 삽입
        response = (
            supabase.table('news')
            .insert(all_news)
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
        

async def main():
    load_dotenv()
    
    supabase_url = os.environ.get('SUPABASE_URL')
    supabase_api_key = os.environ.get('SUPABASE_KEY')
    
    supabase: Client = create_client(supabase_url, supabase_api_key)
    fetch_from_stocks = get_stock_from_db(supabase)
    if not fetch_from_stocks:
        return
    
    end_day = datetime.now()
    start_day = end_day - timedelta(days=13)
    
    all_news = await get_news_data_async(fetch_from_stocks, start_day, end_day)
    
    if all_news:
        save_news_in_db(all_news, supabase)
    
if __name__ == "__main__":
    asyncio.run(main())


# DB 용량 우려로 추후에 하루당 뉴스 기사 개수를 제한하고 싶으면 사용
# def fetch_news_rss_day(query, stock_id, day: datetime, limit: int = 30):
#     """
#     지정된 날짜의 RSS 피드를 파싱하여, 최대 'limit' 개수만큼의 뉴스 아이템을 반환합니다.
#     """
#     start_date = day.strftime("%Y-%m-%d")
#     end_date = (day + timedelta(days=1)).strftime("%Y-%m-%d")
#     url = generate_google_rss_url(query, start_date, end_date)
#     feed = feedparser.parse(url)

#     items = []
#     # ---- [ 핵심 수정 부분 ] ----
#     # feed.entries 리스트를 처음부터 'limit' 개수만큼만 잘라서 순회합니다.
#     for entry in feed.entries[:limit]:
#         try:
#             pub_date = datetime(*entry.published_parsed[:6])
#         except Exception:
#             continue
        
#         items.append({
#             "created_date": pub_date.strftime("%Y-%m-%d %H:%M:%S"), # 시간까지 포함하면 더 좋습니다.
#             "title": adjust_title_by_length_limit(entry.title),
#             "original_url": entry.link,
#             "company_name" : query,
#             "view_count" : 0,
#             "like_count" : 0,
#             "stock_id" : stock_id
#         })

#     return items