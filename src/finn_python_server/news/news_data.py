import os
import feedparser
from datetime import datetime, timedelta
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv

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

def fetch_news_rss_day(query, stock_id, day: datetime):
    start_date = day.strftime("%Y-%m-%d")
    end_date = (day + timedelta(days=1)).strftime("%Y-%m-%d")
    url = generate_google_rss_url(query, start_date, end_date)
    feed = feedparser.parse(url)

    items = []
    for entry in feed.entries:
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

def get_news_data(fetch_from_stocks, start_day, end_day):
    all_news = []
    
    for stock in fetch_from_stocks:
        query = stock['search_keyword']
        stock_id = stock['id']
        
        current_day = start_day

        while current_day <= end_day:
            daily_news = fetch_news_rss_day(query, stock_id, current_day)
            all_news.extend(daily_news)
            current_day += timedelta(days=1)
    
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
        

def main():
    load_dotenv()
    
    supabase_url = os.environ.get('SUPABASE_URL')
    supabase_api_key = os.environ.get('SUPABASE_KEY')
    
    supabase: Client = create_client(supabase_url, supabase_api_key)
    fetch_from_stocks = get_stock_from_db(supabase)
    
    end_day = datetime.now()
    start_day = end_day
    
    all_news = get_news_data(fetch_from_stocks, start_day, end_day)
    
    if all_news:
        save_news_in_db(all_news, supabase)
    
main()


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