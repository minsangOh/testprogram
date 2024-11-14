import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyupbit
from dotenv import load_dotenv
from logging.handlers import TimedRotatingFileHandler

# .env 파일에서 환경 변수 로드
load_dotenv()

# 로그 파일 저장 경로 설정
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_file = os.path.join(log_dir, "trading_log.log")

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = TimedRotatingFileHandler(log_file, when="midnight", interval=1, encoding="utf-8")
handler.suffix = "%Y-%m-%d"
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

logger.info("초단기 매매 전략 시작")

# .env 파일에서 API 키 로드
access_key = os.getenv("UPBIT_ACCESS_KEY")
secret_key = os.getenv("UPBIT_SECRET_KEY")
if not access_key or not secret_key:
    raise ValueError("API 키가 설정되지 않았습니다. .env 파일을 확인하세요.")

# Upbit API 객체 생성
upbit = pyupbit.Upbit(access_key, secret_key)

# 유효한 티커 리스트 캐싱 (1시간에 한 번 갱신)
valid_tickers = pyupbit.get_tickers(fiat="KRW")
last_ticker_update_time = time.time()

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
    return 40 <= recent_rsi <= 60

# 매도 조건 판단 함수
def is_sell_condition(current_price, avg_buy_price, trend):
    if trend == "bull":
        if current_price >= avg_buy_price * 1.005:
            return "profit"
        elif current_price <= avg_buy_price * 0.995:
            return "loss"
    elif trend == "bear":
        if current_price <= avg_buy_price * 0.99:
            return "loss"
        elif current_price >= avg_buy_price * 1.002:
            return "profit"
    elif trend == "sideways":
        if current_price >= avg_buy_price * 1.003:
            return "profit"
        elif current_price <= avg_buy_price * 0.997:
            return "loss"
    return None

# 시장 트렌드 판단 함수
def determine_market_trend(ticker):
    data = pyupbit.get_ohlcv(ticker, interval="minute1")
    if data is None or data.empty:
        return "sideways"
    short_ma = data['close'].rolling(window=12).mean().iloc[-1]
    long_ma = data['close'].rolling(window=26).mean().iloc[-1]
    if short_ma > long_ma:
        return "bull"
    elif short_ma < long_ma:
        return "bear"
    else:
        return "sideways"

# 보유하고 있는 티커 목록 반환 함수
def get_owned_tickers():
    balances = upbit.get_balances()
    owned_tickers = set()
    for balance in balances:
        if isinstance(balance, dict) and 'currency' in balance and balance['currency'] != 'KRW':
            ticker = f"KRW-{balance['currency']}"
            owned_tickers.add(ticker)
    return owned_tickers

# 매수 전략 함수
def buy_strategy():
    global valid_tickers, last_ticker_update_time
    while True:
        try:
            # 티커 목록 갱신 (1시간에 한 번만 갱신)
            current_time = time.time()
            if current_time - last_ticker_update_time > 3600:
                valid_tickers = pyupbit.get_tickers(fiat="KRW")
                last_ticker_update_time = current_time
                logging.info("유효한 티커 리스트 갱신")

            krw_balance = float(upbit.get_balance("KRW"))
            if krw_balance < 5500:
                logging.info("잔고 부족으로 매수 포기")
                time.sleep(11)
                continue

            owned_tickers = get_owned_tickers()
            buy_candidates = []

            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_ticker = {executor.submit(pyupbit.get_ohlcv, ticker, "minute1"): ticker for ticker in valid_tickers}
                for future in as_completed(future_to_ticker):
                    ticker = future_to_ticker[future]
                    try:
                        df = future.result()
                        if df is None or ticker in owned_tickers:
                            continue
                        if is_buy_condition(df):
                            buy_candidates.append(ticker)
                            logging.info(f"{ticker} - 매수 조건 충족")
                    except Exception as e:
                        logging.warning(f"{ticker} 데이터 오류: {e}")

            if buy_candidates:
                for ticker in buy_candidates:
                    owned_tickers = get_owned_tickers()
                    if len(owned_tickers) >= 50:
                        logging.info("최대 보유 코인 수 도달")
                        break
                    if ticker in owned_tickers:
                        logging.info(f"이미 보유 중: {ticker}, 매수 건너뜀")
                        continue
                    krw_balance = float(upbit.get_balance("KRW"))
                    if krw_balance < 5500:
                        logging.info("잔고 부족으로 추가 매수 포기")
                        break
                    logging.info(f"매수할 코인: {ticker}")
                    buy_result = upbit.buy_market_order(ticker, 5500)
                    logging.info(f"매수 완료 - 티커: {ticker}, 결과: {buy_result}")
            time.sleep(11)

        except Exception as e:
            logging.error(f"매수 전략 중 오류 발생: {e}")

# 매도 전략 함수
def sell_strategy():
    while True:
        try:
            for balance in upbit.get_balances():
                if isinstance(balance, dict) and 'currency' in balance and balance['currency'] != 'KRW':
                    ticker = f"KRW-{balance['currency']}"
                    balance_amount = float(balance['balance'])
                    avg_buy_price = float(balance['avg_buy_price'])
                    if balance_amount < 0.0001 or avg_buy_price == 0:
                        continue

                    df = pyupbit.get_ohlcv(ticker, "minute1")
                    if df is not None:
                        current_price = pyupbit.get_current_price(ticker)
                        trend = determine_market_trend(ticker)
                        sell_condition = is_sell_condition(current_price, avg_buy_price, trend)
                        if sell_condition:
                            if sell_condition == "profit":
                                logging.info(f"{ticker} 매도 실행 - 트렌드: {trend}, 수익 실현")
                            elif sell_condition == "loss":
                                logging.info(f"{ticker} 매도 실행 - 트렌드: {trend}, 손절")
                            sell_result = upbit.sell_market_order(ticker, balance['balance'])
                            logging.info(f"매도 완료 - 티커: {ticker}, 결과: {sell_result}")
                    time.sleep(11)

            time.sleep(11)

        except Exception as e:
            logging.error(f"매도 전략 중 오류 발생: {e}")

# 메인 함수
def main():
    buy_thread = threading.Thread(target=buy_strategy, daemon=True)
    sell_thread = threading.Thread(target=sell_strategy, daemon=True)
    buy_thread.start()
    sell_thread.start()

    while True:
        if not buy_thread.is_alive():
            logging.error("매수 스레드가 중지됨, 재시작합니다.")
            buy_thread = threading.Thread(target=buy_strategy, daemon=True)
            buy_thread.start()
        if not sell_thread.is_alive():
            logging.error("매도 스레드가 중지됨, 재시작합니다.")
            sell_thread = threading.Thread(target=sell_strategy, daemon=True)
            sell_thread.start()
        time.sleep(3)

if __name__ == "__main__":
    main()
