#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import random
import time
from datetime import datetime
from collections import defaultdict
import argparse
from tqdm import tqdm
import concurrent.futures
from urllib.parse import urljoin, urlparse

# --- Argument parsing ----------------------------------------------------------------
parser = argparse.ArgumentParser()
parser.add_argument('--start-page', type=int, default=1, help='크롤링 시작 페이지')
parser.add_argument('--end-page', type=int, default=200000, help='크롤링 종료 페이지')
parser.add_argument('--total-posts', type=int, default=50000, help='샘플링할 게시물 수')
parser.add_argument('--interval', type=int, default=120, help='샘플링 간격')
parser.add_argument('--save-intv', type=int, default=10000, help='중간 저장 단위')
args = parser.parse_args()

# --- 환경 설정 -----------------------------------------------------------------------
start_page        = args.start_page
end_page          = args.end_page
TOTAL_POSTS       = args.total_posts
SAMPLING_INTERVAL = args.interval
SAVE_INTERVAL     = args.save_intv
LIST_NUM          = 30  # 한 페이지당 게시물 수, 모바일은 js 동적이라 지정 불가능
BASE_GALLERY      = "bitcoins_new1"
# 모바일 도메인 사용
BASE_LIST_URL     = f"https://m.dcinside.com/board/{BASE_GALLERY}"
BASE_VIEW_URL     = f"https://m.dcinside.com/board/{BASE_GALLERY}"
BASE_FILENAME     = 'dcinside_mobile'

# 실행 환경 확인용
print(f"▶ BASE_LIST_URL: {BASE_LIST_URL}, BASE_VIEW_URL: {BASE_VIEW_URL}")

# 모바일 User-Agent 강제 지정
MOBILE_UA = (
    'Mozilla/5.0 (Linux; Android 10; SM-G970F) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/105.0.0.0 Mobile Safari/537.36'
)

def get_headers(referer=None):
    headers = {'User-Agent': MOBILE_UA, 'Accept-Language': 'ko-KR,ko;q=0.9'}
    if referer:
        headers['Referer'] = referer
    return headers

# 상세페이지 정보 추출

def extract_post_info(session, post_url):
    resp = session.get(post_url, headers=get_headers(referer=post_url), timeout=(5, 30))
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')

    # 본문 내용(모든 태그 포함)을 한 번에 가져와서 합치기
    content_container = soup.select_one('div.gall-thum-btm div.thum-txt')
    if content_container:
        # 모든 하위 텍스트를 줄바꿈(sep='\n')으로 합치고, 앞뒤 공백 제거
        text = content_container.get_text(separator='\n', strip=True)
    else:
        text = '내용 없음'
        
    view_elem  = soup.select_one('span.gall_count')
    reply_elem = soup.select_one('span.gall_reply_num')

    return {
        '내용':       text,
        '상세_조회수': re.sub(r'[^0-9]', '', view_elem.get_text()) if view_elem else '0',
        '상세_댓글수': re.sub(r'[^0-9]', '', reply_elem.get_text()) if reply_elem else '0'
    }
    
# CSV 저장

def save_to_csv(df: pd.DataFrame, filename: str):
    if not filename.endswith('.csv'):
        filename = filename.rstrip('.parquet') + '.csv'
    df.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 저장 → {filename}")

# --- Main ----------------------------------------------------------------------------
if __name__ == '__main__':
    session = requests.Session()
    # 쿠키로 list_num 설정
    session.cookies.set('list_num', str(LIST_NUM), domain='m.dcinside.com', path='/')

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    collected = []

    # 샘플링 인덱스 생성
    estimated_total = end_page * LIST_NUM
    sample_indices = [i * SAMPLING_INTERVAL for i in range(1, TOTAL_POSTS + 1)
                      if i * SAMPLING_INTERVAL <= estimated_total]

    # 페이지별 오프셋 매핑
    page_offsets = defaultdict(list)
    for idx in sample_indices:
        page = (idx - 1) // LIST_NUM + start_page
        offset = (idx - 1) % LIST_NUM
        page_offsets[page].append(offset)

    part = 1
    try:
        # 프로그래스바를 총 샘플링할 게시물 수(TOTAL_POSTS)로 설정
        pbar = tqdm(total=TOTAL_POSTS, desc="수집 진행", unit="개")
        for page in range(start_page, end_page + 1):
            offsets = page_offsets.get(page, [])
            if not offsets:
                continue
            try:
                resp = session.get(
                    BASE_LIST_URL,
                    params={'page': page},
                    headers=get_headers(referer=BASE_LIST_URL),
                    timeout=(5, 30)  # connect, read
                )
                resp.raise_for_status()
            except requests.exceptions.ReadTimeout as e:
                print(f"[WARNING] 페이지 {page} 읽기 타임아웃: {e}. 2초 후 건너뜁니다.")
                time.sleep(2)
                continue
            except requests.exceptions.RequestException as e:
                print(f"[WARNING] 페이지 {page} 요청 실패: {e}. 건너뜁니다.")
                continue

            soup = BeautifulSoup(resp.text, 'html.parser')

            # 모바일 리스트 항목 선택
            items = soup.select('section:nth-of-type(3) ul.gall-detail-lst > li')
            # print(f"[DEBUG] page={page}, items found={len(items)}")
            if not items:
                continue

            # 리스트 메타 및 상세 수집
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                futures = []
                metas = []
                for off in offsets:
                    idx = min(off, len(items) - 1)
                    item = items[idx]

                    # 리스트 정보
                    title   = item.select_one('a.lt span.subjectin')
                    nick_li = item.select_one('a.lt ul li:nth-child(1)')
                    date_li = item.select_one('a.lt ul li:nth-child(2)')
                    views_li= item.select_one('a.lt ul li:nth-child(3)')
                    reco_li = item.select_one('a.lt ul li:nth-child(4)')
                    comm_li = item.select_one('a.rt > span')

                    # href 및 post_id 추출 (쿼리 제거)
                    link_tag = item.select_one('a.lt')
                    if not link_tag:
                        continue
                    href = link_tag.get('href', '')
                    parsed = urlparse(href)
                    post_id = parsed.path.rstrip('/').split('/')[-1]
                    post_url = href if href.startswith('http') else urljoin(BASE_VIEW_URL + '/', href)
                    
                    # 날짜 가공: “M.D” 형식일 때 뒤에 0 붙이기
                    raw_date = date_li.get_text(strip=True) if date_li else ''
                    if re.match(r'^\d+\.\d$', raw_date):
                        raw_date += '0'

                    meta = {
                        '글번호':      post_id,
                        '제목':        title.get_text(strip=True) if title else '',
                        '닉네임':      nick_li.get_text(strip=True) if nick_li else '',
                        '날짜':        raw_date,
                        '목록_조회수': re.sub(r'[^0-9]', '', views_li.get_text()) if views_li else '',
                        '목록_추천수': re.sub(r'[^0-9]', '', reco_li.get_text())  if reco_li else '',
                        '목록_댓글수': re.sub(r'[^0-9]', '', comm_li.get_text())  if comm_li else ''
                    }
                    metas.append(meta)
                    futures.append(executor.submit(extract_post_info, session, post_url))

                for meta, fut in zip(metas, futures):
                    try:
                        detail = fut.result(timeout=15)
                        meta.update(detail)
                        collected.append(meta)
                        pbar.update(1)
                    except Exception:
                        pass

            # 중간 저장
            if len(collected) >= SAVE_INTERVAL:
                final = f"{BASE_FILENAME}_{timestamp}_{start_page}.csv"
                save_to_csv(pd.DataFrame(collected), final)
                collected.clear()
                part += 1

            time.sleep(random.uniform(0.5, 1.0))
            if len(collected) >= TOTAL_POSTS:
                break

    except KeyboardInterrupt:
        print('강제 종료, 저장 시도...')
    finally:
        pbar.close()
        if collected:
            final = f"{BASE_FILENAME}_{timestamp}_{start_page}.csv"
            save_to_csv(pd.DataFrame(collected), final)
        else:
            print('저장할 데이터 없음.')
