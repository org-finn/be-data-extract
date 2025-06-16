import os
import feedparser
from datetime import datetime, timedelta
import pandas as pd
from tqdm import tqdm
import time  # 순차 요청을 위한 time 라이브러리 임포트
import requests # 비동기 aiohttp 대신 동기 requests 라이브러리 임포트
from config import STOCK_LIST

# --- 헬퍼 함수 (변경 없음) ---
def generate_google_rss_url(query, start_date, end_date):
    base_url = "https://news.google.com/rss/search?"
    q = f"q={query}+after:{start_date}+before:{end_date}"
    params = "&hl=en-US&gl=US&ceid=US:en"
    return base_url + q

def adjust_title_by_length_limit(title):
    return (title[:97] + '...') if len(title) > 100 else title

def remove_duplicate_titles_by_prefix(all_news, prefix_length=50):
    seen = set()
    keep_rows = []
    for news in all_news:
        prefix = news["title"][:prefix_length].strip().lower()
        if prefix not in seen:
            seen.add(prefix)
            keep_rows.append(news)
    return keep_rows

# --- 데이터 수집 함수 (동기 코드로 변경) ---

def fetch_news_rss_day_sync(query, day: datetime, limit: int = 30):
    """[동기] 지정된 하루치 뉴스 데이터를 순차적으로 가져옵니다."""
    start_date = day.strftime("%Y-%m-%d")
    end_date = (day + timedelta(days=1)).strftime("%Y-%m-%d")
    url = generate_google_rss_url(query, start_date, end_date)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    items = []
    try:
        # async with session.get 대신 requests.get 사용
        response = requests.get(url, timeout=20, headers=headers)
        
        # HTTP 상태 코드 확인
        if response.status_code != 200:
            print(f"Error fetching {query} on {start_date}: {response.status_code}")
            return []  # 실패 시 빈 리스트 반환

        feed = feedparser.parse(response.text)
        
        for entry in feed.entries[:limit]:
            try:
                pub_date = datetime(*entry.published_parsed[:6])
                items.append({
                    "date": pub_date.strftime("%Y-%m-%d"),
                    "title": entry.title,
                    "link": entry.link,
                    "source": entry.source.title
                })
            except Exception:
                continue
    except Exception as e:
        print(f"Error fetching {query} on {start_date}: {e}")

    return items

def get_news_data_sync(stock_code, start_day, end_day):
    """[동기] 한 종목에 대한 모든 뉴스 데이터를 순차적으로 수집하고 진행 상황을 표시합니다."""
    all_news = []
    total_days = (end_day - start_day).days + 1

    # ⭐ 1. for 반복문의 range를 tqdm으로 감싸줍니다.
    # ⭐ 2. desc 파라미터를 사용하여 어떤 종목의 진행률인지 표시해줍니다.
    for i in tqdm(range(total_days), desc=f"[{stock_code}] 개별 날짜 진행률"):
        current_day = start_day + timedelta(days=i)
        
        # 하루치 데이터를 순차적으로 호출
        daily_news = fetch_news_rss_day_sync(stock_code, current_day)
        if daily_news:
            all_news.extend(daily_news)

    unique_news = remove_duplicate_titles_by_prefix(all_news, prefix_length=50)
    return unique_news

def get_stock_price_for_train(stock_code):
    """[동기] 학습용 데이터를 수집하고 저장합니다."""
    start_day = datetime(2024, 1, 1)
    end_day = datetime(2024, 12, 31)
    
    # 동기 함수 호출 (await 제거)
    all_news = get_news_data_sync(stock_code, start_day, end_day)
    if not all_news:
        print(f"[{stock_code}] Train 뉴스가 없습니다.")
        return

    news_df = pd.DataFrame(all_news)
    news_df.sort_values(by="date", inplace=True)
    news_df = news_df[["date", "title", "link", "source"]]
    news_df["date"] = pd.to_datetime(news_df["date"])

    output_path = os.path.expanduser(f'~/Downloads/finn_data/news/{stock_code}_news_train.csv')
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    news_df.to_csv(output_path, index=False)

def get_stock_price_for_test(stock_code):
    """[동기] 테스트용 데이터를 수집하고 저장합니다."""
    start_day = datetime(2025, 1, 1)
    end_day = datetime(2025, 5, 31)

    # 동기 함수 호출 (await 제거)
    all_news = get_news_data_sync(stock_code, start_day, end_day)
    if not all_news:
        print(f"[{stock_code}] Test 뉴스가 없습니다.")
        return
        
    news_df = pd.DataFrame(all_news)
    news_df.sort_values(by="date", inplace=True)
    news_df = news_df[["date", "title", "link", "source"]]
    news_df["date"] = pd.to_datetime(news_df["date"])
    
    output_path = os.path.expanduser(f'~/Downloads/finn_data/news/{stock_code}_news_test.csv')
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    news_df.to_csv(output_path, index=False)

# --- 메인 실행 함수 (동기 코드로 변경) ---

def main():
    """메인 실행 함수"""
    print("학습용 데이터 순차적 다운로드를 시작합니다 (종목별 처리).")
    for stock_code in tqdm(STOCK_LIST, desc="전체 종목 진행률 (Train)"):
        # 각 종목 처리가 끝날 때까지 기다림 (await 제거)
        get_stock_price_for_train(stock_code)
    print("\n학습용 데이터 다운로드 완료.")

    print("\n테스트용 데이터 순차적 다운로드를 시작합니다 (종목별 처리).")
    for stock_code in tqdm(STOCK_LIST, desc="전체 종목 진행률 (Test)"):
        get_stock_price_for_test(stock_code)
    print("\n모든 데이터 다운로드가 완료되었습니다.")

if __name__ == "__main__":
    # asyncio.run() 대신 main() 직접 호출
    main()