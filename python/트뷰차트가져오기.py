import pandas as pd
from datetime import datetime, timezone
import math

# 1) 처리할 심볼 목록

# 바이낸스 비트코인 BINANCE:BTCUSDT
# 바이낸스 이더리움 BINANCE:ETHUSDT
# 바이낸스 솔라나 BINANCE:SOLUSD

# 나스닥 FX:NAS100
# 금 FX:XAUUSD

# 미국 M0 ECONOMICS:USM0
# 미국 M1 ECONOMICS:USM1
# 미국 M2 ECONOMICS:USM2

# 중국 M0 ECONOMICS:CNM0
# 중국 M1 ECONOMICS:CNM1
# 중국 M2 ECONOMICS:CNM2

from price_loaders.tradingview import load_asset_price

symbols = [
    "BINANCE:BTCUSDT", "BINANCE:ETHUSDT", "BINANCE:SOLUSDT",
    "FX:NAS100", "FX:XAUUSD",
    "ECONOMICS:USM0", "ECONOMICS:USM1", "ECONOMICS:USM2",
    "ECONOMICS:CNM0", "ECONOMICS:CNM1", "ECONOMICS:CNM2"
]

symbols = [
'BINANCE:SOLUSD'
]

# 2) 날짜 범위 정의
start_str = "2019-12-01"
end_str   = "2025-05-04"
start_date = datetime.fromisoformat(start_str).replace(tzinfo=timezone.utc)
end_date   = datetime.fromisoformat(end_str).replace(tzinfo=timezone.utc)
look_back_days = (end_date - start_date).days + 1

# 3) 각 심볼별 처리
for symbol in symbols:
    symbol = symbol.strip()
    print(f"\n=== Processing {symbol} ===")

    # 보호 로직: 1D → 1W → 1M 순으로 시도
    time_frames = ["1D", "1W", "1M"]
    for tf in time_frames:
        if tf == "1D":
            bars = look_back_days
        elif tf == "1W":
            bars = math.ceil(look_back_days / 7)
        else:  # "1M"
            bars = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month) + 1

        try:
            dfm = load_asset_price(
                symbol=symbol,
                look_back_bars=bars,
                time_frame=tf,
                timezone=None
            )
            print(f"  Loaded {tf} (bars={bars})")
            break

        except KeyError as e:
            if 'price' in str(e):
                print(f"  {tf} 데이터 없어서 다음 시도")
                continue
            else:
                raise

        except OSError as e:
            print(f"  {tf} timestamp 에러 ({e}), 다음 시도")
            continue

    else:
        print(f"  ❌ {symbol} 모든 timeframe 실패, 건너뜁니다.")
        continue

    # 4) UTC 타임스탬프 → datetime, tz 제거
    dfm['date'] = pd.to_datetime(dfm['time'], unit='s', utc=True).dt.tz_localize(None)

    # 5) 날짜 범위로 컷오프
    dfm = dfm[
        (dfm['date'] >= start_date.replace(tzinfo=None)) &
        (dfm['date'] <= end_date.replace(tzinfo=None))
    ]

    # 6) 필수 컬럼 보장 (volume은 0으로, 나머지는 pd.NA)
    for col in ["open", "high", "low", "close", "volume"]:
        if col not in dfm.columns:
            dfm[col] = 0 if col == "volume" else pd.NA

    # 7) date를 인덱스로 설정하고 오름차순 정렬
    dfm.set_index('date', inplace=True)
    dfm.sort_index(inplace=True)

    # 8) 일별 리샘플 (월간 캔들은 하루하루 동일 값으로 채움)
    dfd = dfm.resample('D').ffill().reset_index()

    # 9) 불필요한 time 컬럼 제거
    if 'time' in dfd.columns:
        dfd.drop(columns=['time'], inplace=True)

    # 10) 내림차순 정렬 및 CSV 저장
    dfd.sort_values('date', ascending=False, inplace=True)
    filename = f"{symbol.replace(':', '_')}_daily_UTC.csv"
    # dfd.to_csv(filename, index=False, encoding='utf-8')
    print(f"  ✅ Saved {filename}")
