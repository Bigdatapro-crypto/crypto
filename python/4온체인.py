import os
import time
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ==============================================
# 0) 사용자 설정
# ==============================================

BASE_FILTER_URL = (
    "https://etherscan.io/advanced-filter?"
    "tkn=0xdac17f958d2ee523a2206206994597c13d831ec7"
    "&txntype=2"
    "&amt=16000000~999999999"
    "&age=2020-01-01~2025-04-30"
    "&ps=100"
)

MAX_PAGE = 350
PAGE_LOAD_TIMEOUT = 60
DOWNLOAD_TIMEOUT = 60
REQUEST_DELAY = 1

DOWNLOAD_DIR = os.path.join(os.getcwd(), "etherscan_downloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)


# ==============================================
# 1) ChromeOptions 설정 (udc 사용)
# ==============================================
chrome_options = uc.ChromeOptions()
prefs = {
    "download.default_directory": DOWNLOAD_DIR,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
}
chrome_options.add_experimental_option("prefs", prefs)
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
# chrome_options.add_argument("--headless")  # 필요 시 해제

driver = uc.Chrome(options=chrome_options)
driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)


# ==============================================
# 2) 다운로드 완료 대기 함수
# ==============================================
def wait_for_download_complete(download_dir, timeout=DOWNLOAD_TIMEOUT):
    end_time = time.time() + timeout
    while True:
        if not any(fname.endswith(".crdownload") for fname in os.listdir(download_dir)):
            return True
        if time.time() > end_time:
            return False
        time.sleep(1)


# ==============================================
# 3) 페이지별 다운로드 로직
# ==============================================
try:
    for page_num in range(1, MAX_PAGE + 1):
        page_url = f"{BASE_FILTER_URL}&p={page_num}"
        print(f"\n[{page_num}/{MAX_PAGE}] 페이지 열기 → {page_url}")

        # (1) 페이지 이동
        try:
            driver.get(page_url)
        except Exception as e:
            print(f"  → 페이지 로딩 예외: {e}. 계속 진행…")

        # (2) 결과 테이블이 표시될 때까지 대기
        print("  → 결과 테이블 로딩 대기 중…")
        try:
            WebDriverWait(driver, PAGE_LOAD_TIMEOUT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#tblResult table"))
            )
        except Exception:
            print(f"  [경고] {PAGE_LOAD_TIMEOUT}s 내에 테이블 로딩되지 않음. 다음 페이지로…")
            continue

        # (3) Download 버튼이 클릭 가능해질 때까지 대기
        try:
            download_btn = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.ID, "btnExportQuickTableToCSV"))
            )
        except Exception:
            print(f"  [경고] 페이지 {page_num}에서 Download 버튼을 찾지 못함. 다음 페이지로…")
            continue

        # (4) 다운로드 전후 폴더 상태 기록
        before_files = set(os.listdir(DOWNLOAD_DIR))

        # (5) 버튼 클릭하여 CSV 다운로드
        print("  → 'Download Page Data' 버튼 클릭…")
        download_btn.click()

        # (6) 다운로드 완료 대기
        if wait_for_download_complete(DOWNLOAD_DIR, DOWNLOAD_TIMEOUT):
            after_files = set(os.listdir(DOWNLOAD_DIR))
            new_files = after_files - before_files

            if new_files:
                latest_file = new_files.pop()
                src = os.path.join(DOWNLOAD_DIR, latest_file)
                dst = os.path.join(DOWNLOAD_DIR, f"page_{page_num}.csv")
                os.rename(src, dst)
                print(f"  → 다운로드 완료 및 파일명 변경 → '{dst}'")
            else:
                print(f"  [경고] 새 파일이 감지되지 않아 이름 변경을 못했습니다.")
        else:
            print(f"  [경고] 페이지 {page_num} 다운로드 ({DOWNLOAD_TIMEOUT}s) 초과. 다음 페이지로…")

        # (7) 페이지 간 짧게 대기
        time.sleep(REQUEST_DELAY)

    print("\n▶ 모든 페이지 다운로드 완료 또는 중단되었습니다.")
finally:
    driver.quit()
