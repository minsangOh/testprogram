import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from logging.handlers import TimedRotatingFileHandler
from queue import Queue

import pyupbit
from dotenv import load_dotenv

# .env에서 환경 변수 로드
load_dotenv()

# 로그 파일 저장 경로 설정
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_file = os.path.join(log_dir, "trading_log.log")

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 로그 파일은 매일 자정에 새로운 파일로 로테이션되도록 설정
handler = TimedRotatingFileHandler(log_file, when="midnight", interval=1, encoding="utf-8")
handler.suffix = "%Y-%m-%d"
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# 콘솔에도 로그 출력
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

logger.info("단기 매매 시스템 시작")

# .env 파일에서 API 키 로드
access_key = os.getenv("UPBIT_ACCESS_KEY")
secret_key = os.getenv("UPBIT_SECRET_KEY")
if not access_key or not secret_key:
    raise ValueError("API 키가 설정되지 않았습니다. .env 파일을 확인하세요.")

# Upbit API 객체 생성
upbit = pyupbit.Upbit(access_key, secret_key)

# 유효한 티커 리스트 관리
valid_tickers = pyupbit.get_tickers(fiat="KRW")
last_ticker_update_time = time.time()

# 큐 초기화
buy_queue = Queue()
sell_queue = Queue()

# RSI 계산 함수
def calculate_rsi(data, period=14):
    delta = data['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.iloc[-1]

# 매수 조건 판단 함수
def is_buy_condition(data):
    recent_rsi = calculate_rsi(data)
    return 30 < recent_rsi <= 60

# 매도 조건 판단 함수
def is_sell_condition(current_price, avg_buy_price, trend):
    fee = 1.0005
    if trend == "bull":
        if current_price >= avg_buy_price * fee * 1.005:
            return "profit"
        elif current_price <= avg_buy_price * fee * 0.995:
            return "loss"
    elif trend == "bear":
        if current_price >= avg_buy_price * fee * 1.002:
            return "profit"
        elif current_price <= avg_buy_price * fee * 0.99:
            return "loss"
    elif trend == "sideways":
        if current_price >= avg_buy_price * fee * 1.003:
            return "profit"
        elif current_price <= avg_buy_price * fee * 0.997:
            return "loss"
    return None

# 시장 트렌드 판단 함수
def determine_market_trend(ticker):
    data = pyupbit.get_ohlcv(ticker, interval="minute1")
    if data is None or data.empty:
        return "sideways"
    short_ma = data['close'].rolling(window=7).mean().iloc[-1]
    long_ma = data['close'].rolling(window=21).mean().iloc[-1]
    if short_ma > long_ma:
        return "bull"
    elif short_ma < long_ma:
        return "bear"
    else:
        return "sideways"

# 매수 전략
def buy_strategy():
    global valid_tickers, last_ticker_update_time
    while True:
        try:
            current_time = time.time()
            if current_time - last_ticker_update_time > 3600:
                valid_tickers = pyupbit.get_tickers(fiat="KRW")
                last_ticker_update_time = current_time
                logger.info("유효한 티커 리스트 갱신")

            krw_balance = float(upbit.get_balance("KRW"))
            if krw_balance < 5500:
                time.sleep(1)
                continue

            owned_tickers = get_owned_tickers()
            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_ticker = {executor.submit(pyupbit.get_ohlcv, ticker, "minute1"): ticker for ticker in valid_tickers}
                for future in as_completed(future_to_ticker):
                    ticker = future_to_ticker[future]
                    try:
                        df = future.result()
                        if df is None or ticker in owned_tickers:
                            continue
                        if is_buy_condition(df):
                            buy_queue.put(ticker)
                    except Exception as e:
                        logger.warning(f"{ticker} 데이터 오류: {e}")
            time.sleep(1)
        except Exception as e:
            logger.error(f"매수 전략 중 오류 발생: {e}")

# 매도 전략
def sell_strategy():
    while True:
        try:
            for balance in upbit.get_balances():
                if isinstance(balance, dict) and 'currency' in balance and balance['currency'] != 'KRW':
                    ticker = f"KRW-{balance['currency']}"
                    balance_amount = float(balance['balance'])
                    avg_buy_price = float(balance['avg_buy_price'])
                    current_price = pyupbit.get_current_price(ticker)
                    trend = determine_market_trend(ticker)
                    if is_sell_condition(current_price, avg_buy_price, trend):
                        sell_queue.put(ticker)
            time.sleep(1)
        except Exception as e:
            logger.error(f"매도 전략 중 오류 발생: {e}")

# 큐 처리 함수
def process_queues():
    while True:
        # 매수 큐 처리
        while not buy_queue.empty():
            ticker = buy_queue.get()
            krw_balance = float(upbit.get_balance("KRW"))
            if krw_balance >= 5500:
                upbit.buy_market_order(ticker, 5500)
                logger.info(f"매수 완료 - 티커: {ticker}")
            # 중복 제거
            while not buy_queue.empty() and buy_queue.queue.count(ticker) > 1:
                buy_queue.queue.remove(ticker)

        # 매도 큐 처리
        while not sell_queue.empty():
            ticker = sell_queue.get()
            for balance in upbit.get_balances():
                if f"KRW-{balance['currency']}" == ticker:
                    upbit.sell_market_order(ticker, balance['balance'])
                    logger.info(f"매도 완료 - 티커: {ticker}")
            # 중복 제거
            while not sell_queue.empty() and sell_queue.queue.count(ticker) > 1:
                sell_queue.queue.remove(ticker)
        time.sleep(0.5)

# 보유 티커 조회 함수
def get_owned_tickers():
    balances = upbit.get_balances()
    return {f"KRW-{b['currency']}" for b in balances if b['currency'] != 'KRW'}

# 메인 함수
def main():
    threading.Thread(target=buy_strategy, daemon=True).start()
    threading.Thread(target=sell_strategy, daemon=True).start()
    threading.Thread(target=process_queues, daemon=True).start()

    while True:
        time.sleep(3)

if __name__ == "__main__":
    main()
