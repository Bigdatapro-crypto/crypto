#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import time
import datetime
import csv

# ——— Config ———
MAX_SAMPLES = 10_000
CHUNK_SIZE  = 10_000
PREFIX      = "coinpan_free_sample"
BASE_URL    = "https://coinpan.com/index.php?mid=free&page={}"
PAGE_STEP   = 50
TIMEOUT     = 20  # seconds
HEADERS     = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/136.0.7103.114 Safari/537.36",
    "Referer": "https://coinpan.com/index.php?mid=free"
}

def parse_one_row(soup):
    # 1) 공지글(notice) 제외한 일반 글 목록 수집
    rows = [
        tr for tr in soup.select("#board_list table tbody tr")
        if "notice" not in tr.get("class", [])
    ]
    if len(rows) < 6:
        return None
    row = rows[5]  # 6번째 일반 글

    # 2) 필드 추출
    no_el        = row.select_one("td.no span")
    title_el     = row.select_one("td.title a")
    comments_el  = row.select_one("td.title > a:nth-child(2) > span")
    date_el      = row.select_one("td.time span span") or row.select_one("td.time")
    views_el     = row.select_one("td.readed span")
    votes_el     = row.select_one("td.voted span")

    no       = no_el.get_text(strip=True) if no_el else ""
    title    = title_el.get_text(strip=True) if title_el else ""
    comments = comments_el.get_text(strip=True) if comments_el else ""
    date     = date_el.get_text(strip=True)    if date_el else ""
    views    = views_el.get_text(strip=True)   if views_el else ""
    votes    = votes_el.get_text(strip=True).split('-')[0] if votes_el else ""

    # 3) 절대 URL 구성
    href = title_el["href"] if title_el and title_el.has_attr("href") else ""
    url  = "https://coinpan.com" + href if href.startswith("/") else href

    # 4) 상세 페이지 전체 <p> 단락 합치기
    content = ""
    if url:
        try:
            r2    = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            soup2 = BeautifulSoup(r2.text, "html.parser")
            paras = soup2.select(
                ".board_read.rd .section_wrap.section_border_0 > div > div > p"
            )
            content = "\n\n".join(p.get_text(strip=True) for p in paras)
        except Exception:
            pass

    return {
        "no":       no,
        "title":    title,
        "comments": comments,
        "date":     date,
        "views":    views,
        "votes":    votes,
        "url":      url,
        "content":  content
    }

def save_chunks_to_csv(samples, last_page):
    total     = len(samples)
    num_files = (total + CHUNK_SIZE - 1) // CHUNK_SIZE
    ts        = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    for i in range(num_files):
        start = i * CHUNK_SIZE
        end   = min(start + CHUNK_SIZE, total)
        chunk = samples[start:end]
        fname = f"{PREFIX}_page{last_page}_part{i+1}_{ts}.csv"

        with open(fname, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["no","title","comments","date","views","votes","url","content"]
            )
            writer.writeheader()
            writer.writerows(chunk)

        print(f"[저장] {fname}: {start+1}–{end}")

def crawl():
    # 1) 세션 생성 & 헤더 설정
    session = requests.Session()
    session.headers.update(HEADERS)

    samples    = []
    start_time = time.time()

    # 2) 사용자 입력
    try:
        p    = input("이동할 시작 페이지 번호를 입력하세요 (기본 5): ").strip()
        page = int(p) if p.isdigit() else PAGE_STEP
    except:
        page = PAGE_STEP

    # 3) 초기 요청으로 쿠키/세션 셋업
    try:
        session.get(BASE_URL.format(page), timeout=TIMEOUT)
    except:
        pass

    last_page = page
    try:
        while len(samples) < MAX_SAMPLES:
            last_page = page
            try:
                url  = BASE_URL.format(page)
                resp = session.get(url, timeout=TIMEOUT)
                print(f"▶ GET {resp.url} [{resp.status_code}]")

                soup = BeautifulSoup(resp.text, "html.parser")
                item = parse_one_row(soup)
                if item:
                    samples.append(item)
                    elapsed = time.time() - start_time
                    avg     = elapsed / len(samples)
                    eta     = str(datetime.timedelta(seconds=int(avg * (MAX_SAMPLES - len(samples)))))
                    print(f"[진행] 페이지 {page} → 샘플 {len(samples)}/{MAX_SAMPLES} "
                          f"(경과 {int(elapsed)}s, 남은 예상 {eta})")

            except requests.exceptions.RequestException as e:
                print(f"[WARN] 페이지 {page} 로드 실패: {e} → 스킵")
            except Exception as e:
                print(f"[ERROR] 페이지 {page} 처리 중 예외 발생: {e} → 스킵")
            finally:
                page += PAGE_STEP

    except KeyboardInterrupt:
        print("\n[INFO] Ctrl+C 감지 – 저장을 진행합니다...")

    finally:
        print(f"총 샘플 수집 완료: {len(samples)}")
        save_chunks_to_csv(samples, last_page)

if __name__ == "__main__":
    crawl()