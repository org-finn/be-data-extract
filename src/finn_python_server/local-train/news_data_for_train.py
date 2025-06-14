import os
import feedparser
from datetime import datetime, timedelta
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
from tqdm.asyncio import tqdm 
import asyncio 
import aiohttp 
from config import STOCK_LIST

def generate_google_rss_url(query, start_date, end_date):
    base_url = "https://news.google.com/rss/search?"
    q = f"q={query}+after:{start_date}+before:{end_date}"
    params = "&hl=en-US&gl=US&ceid=US:en"
    return base_url + q + params

def adjust_title_by_length_limit(title):
    return (title[:97] + '...') if len(title) > 100 else title

async def fetch_news_rss_day_async(session, query, day: datetime, limit: int = 30):
    
    start_date = day.strftime("%Y-%m-%d")
    end_date = (day + timedelta(days=1)).strftime("%Y-%m-%d")
    url = generate_google_rss_url(query, start_date, end_date)

    try:
        async with session.get(url, timeout=20) as response:
            if response.status != 200:
                print(f"Request failed for {query} on {start_date} with status: {response.status}")
                exit
            
            feed_text = await response.text()
            feed = feedparser.parse(feed_text)
            
            items = []
            for entry in feed.entries[:limit]:
                pub_date = datetime(*entry.published_parsed[:6])
                items.append({
                    "date": pub_date.strftime("%Y-%m-%d"),
                    "title": entry.title,
                    "link": entry.link,
                    "source": entry.source.title
                })
 
        return items
    
    except Exception as e:
        print(f"Error fetching {query} on {start_date}: {e}") # 디버깅 시 사용
        exit

def remove_duplicate_titles_by_prefix(all_news, prefix_length=50):
    seen = set()
    keep_rows = []

    for news in all_news:
        prefix = news["title"][:prefix_length].strip().lower()
        if prefix not in seen:
            seen.add(prefix)
            keep_rows.append(news)

    return keep_rows

async def get_news_data_async(stock_code, start_day, end_day):
    """[asyncio] 모든 뉴스 수집 작업을 비동기적으로 동시에 실행합니다."""
    tasks = []
    async with aiohttp.ClientSession() as session:
            
        total_days = (end_day - start_day).days + 1
        for i in range(total_days):
            current_day = start_day + timedelta(days=i)
            # 각 날짜와 주식에 대한 비동기 작업을 생성하여 리스트에 추가
            task = fetch_news_rss_day_async(session, stock_code, current_day)
            tasks.append(task)
        
        # tqdm.gather를 사용하여 모든 작업을 동시에 실행하고 진행 상황을 표시
        results = await tqdm.gather(*tasks, desc="뉴스 동시 수집 중")

    # 결과는 리스트의 리스트 형태이므로, 하나의 리스트로 펼쳐줍니다.
    all_news = [item for sublist in results for item in sublist]
    
    print("중복 제거 작업 진행 중...")
    all_news = remove_duplicate_titles_by_prefix(all_news, prefix_length=50)
    
    return all_news


async def get_stock_price_for_train(stock_code):
    # 문자열 대신 datetime 객체 사용
    start_day = datetime(2024, 1, 1)
    end_day = datetime(2024, 12, 31)
    
    # get_news_data_async는 datetime 객체를 인자로 받습니다.
    all_news = await get_news_data_async(stock_code, start_day, end_day)
    if not all_news: # 뉴스가 없으면 빈 DataFrame 처리
        print(f"[{stock_code}] Train 뉴스가 없습니다.")
        return

    news_df = pd.DataFrame(all_news)
    news_df.sort_values(by="date", inplace=True)
    news_df = news_df[["date", "title", "link", "source"]]
    news_df["date"] = pd.to_datetime(news_df["date"])

    # 파일 경로와 이름 동적으로 생성 및 저장
    output_path = os.path.expanduser(f'~/Downloads/finn_data/news/{stock_code}_news_train.csv')
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    news_df.to_csv(output_path, index=False)
    # print(f"✅ 저장 완료: {os.path.basename(output_path)}") # 상세 로그가 필요할 경우 사용

async def get_stock_price_for_test(stock_code):
    start_day = datetime(2025, 1, 1)
    end_day = datetime(2025, 5, 31)

    all_news = await get_news_data_async(stock_code, start_day, end_day)
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
    # print(f"✅ 저장 완료: {os.path.basename(output_path)}")
    

async def main():
    print("학습용 데이터 순차적 다운로드를 시작합니다 (종목별 처리).")
    # 바깥쪽 for문에 일반 tqdm을 적용하여 전체 종목에 대한 진행률을 표시합니다.
    for stock_code in tqdm(STOCK_LIST, desc="전체 종목 진행률 (Train)"):
        # 각 종목 처리 함수가 끝날 때까지 await로 기다립니다.
        # 이렇게 하면 한 번에 한 종목씩 순차적으로 실행됩니다.
        await get_stock_price_for_train(stock_code)
    print("\n학습용 데이터 다운로드 완료.")

    print("\n테스트용 데이터 순차적 다운로드를 시작합니다 (종목별 처리).")
    for stock_code in tqdm(STOCK_LIST, desc="전체 종목 진행률 (Test)"):
        await get_stock_price_for_test(stock_code)
    print("\n모든 데이터 다운로드가 완료되었습니다.")


if __name__ == "__main__":
    asyncio.run(main())