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

# 로그 파일 저장 경로
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)  # logs 디렉터리 생성

# 로그 파일 이름 설정
log_file = os.path.join(log_dir, "trading_log.log")

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# TimedRotatingFileHandler 설정: 매일 자정마다 새로운 로그 파일 생성
handler = TimedRotatingFileHandler(log_file, when="midnight", interval=1, encoding="utf-8")
handler.suffix = "%Y-%m-%d"  # 파일명에 일자 추가
handler.setLevel(logging.INFO)

# 로그 포맷 설정
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# 파일 핸들러 추가
logger.addHandler(handler)

# 콘솔 출력 핸들러 설정
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# 테스트 로그 메시지
logger.info("로그 파일이 일자별로 생성됩니다.")

# 환경 변수에서 API 키 읽기
access_key = os.getenv("UPBIT_ACCESS_KEY")
secret_key = os.getenv("UPBIT_SECRET_KEY")
if not access_key or not secret_key:
    raise ValueError("API 키가 설정되지 않았습니다. .env 파일을 확인하세요.")

# 업비트 API 객체 생성
upbit = pyupbit.Upbit(access_key, secret_key)
valid_tickers = pyupbit.get_tickers(fiat="KRW")
logging.info(f"유효한 티커 리스트: {valid_tickers}")

# RSI 계산 함수
def calculate_rsi(data, period=14):
    delta = data['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.iloc[-1]

# 골드크로스 체크 함수
def is_golden_cross(data, short_period=5, long_period=20):
    short_ma = data['close'].rolling(window=short_period).mean()
    long_ma = data['close'].rolling(window=long_period).mean()
    return short_ma.iloc[-2] < long_ma.iloc[-2] and short_ma.iloc[-1] > long_ma.iloc[-1]

# 스토캐스틱 계산 함수
def calculate_stochastic(data, k_period=14, d_period=3):
    low_min = data['low'].rolling(window=k_period).min()
    high_max = data['high'].rolling(window=k_period).max()
    k_value = 100 * ((data['close'] - low_min) / (high_max - low_min))
    d_value = k_value.rolling(window=d_period).mean()
    return k_value.iloc[-1], d_value.iloc[-1]

# 시장 상황 판단 함수 - 티커별로 판단 (단기 매매에 맞게 수정)
def determine_market_trend(ticker):
    data = pyupbit.get_ohlcv(ticker, interval="minute5")

    if data is None or data.empty:
        # logging.warning(f"{ticker}의 시장 상황을 판단할 수 없음")
        return "sideways"

    short_period = 12  # 12 * 5분 = 60분 이동평균선
    long_period = 26   # 26 * 5분 = 130분 이동평균선

    short_ma = data['close'].rolling(window=short_period).mean().iloc[-1]
    long_ma = data['close'].rolling(window=long_period).mean().iloc[-1]

    if short_ma > long_ma:
        return "bull"
    elif short_ma < long_ma:
        return "bear"
    else:
        return "sideways"

# 현재 지갑에 보유 중인 코인을 가져오는 함수
def get_owned_tickers():
    balances = upbit.get_balances()
    owned_tickers = set()
    for balance in balances:
        if isinstance(balance, dict) and 'currency' in balance and balance['currency'] != 'KRW':
            ticker = f"KRW-{balance['currency']}"
            owned_tickers.add(ticker)
    return owned_tickers

# 매수 및 매도 로직
def trading_strategy():
    while True:
        try:
            krw_balance = float(upbit.get_balance("KRW"))
            if krw_balance < 5500:
                logging.info("잔고가 부족하여 매수를 포기합니다.")
                time.sleep(1)
                continue

            tickers = pyupbit.get_tickers(fiat="KRW")
            owned_tickers = get_owned_tickers()  # 지갑에 있는 코인 목록
            buy_candidates = []

            with ThreadPoolExecutor(max_workers=10) as executor:
                future_to_ticker = {executor.submit(pyupbit.get_ohlcv, ticker, "minute5"): ticker for ticker in tickers}
                for future in as_completed(future_to_ticker):
                    ticker = future_to_ticker[future]
                    try:
                        df = future.result()
                        if df is None or ticker in owned_tickers:
                            continue  # 보유 중인 코인은 매수 후보에서 제외

                        # 각 코인별 시장 상황 판단
                        market_trend = determine_market_trend(ticker)
                        # logging.info(f"{ticker}의 현재 시장 상황: {market_trend}")

                        recent_rsi = calculate_rsi(df)
                        k_value, d_value = calculate_stochastic(df)

                        # 시장 상황별 매수 조건
                        if market_trend == "bull":  # 상승장
                            if 50 <= recent_rsi < 70 and is_golden_cross(df, short_period=5, long_period=15) and k_value > d_value:
                                buy_candidates.append(ticker)
                                logging.info(f"{ticker} - 상승장 매수 조건 충족")

                        elif market_trend == "bear":  # 하락장
                            if 30 <= recent_rsi <= 40 and is_golden_cross(df, short_period=3, long_period=10):
                                buy_candidates.append(ticker)
                                logging.info(f"{ticker} - 하락장 매수 조건 충족")

                        elif market_trend == "sideways":  # 횡보장
                            if recent_rsi < 40 and k_value < 20 and k_value > d_value:
                                buy_candidates.append(ticker)
                                logging.info(f"{ticker} - 횡보장 매수 조건 충족")

                    except Exception as e:
                        logging.warning(f"{ticker} 데이터 오류: {e}")

            # 매수 로직
            if buy_candidates:
                for ticker in buy_candidates:
                    owned_tickers = get_owned_tickers()  # 매수 시점마다 보유 중인 코인 수 확인
                    if len(owned_tickers) >= 13:  # 최대 13개의 코인만 보유
                        logging.info("최대 보유 코인 수에 도달하여 매수를 중지합니다.")
                        break

                    if ticker in owned_tickers:  # 이미 보유 중인 코인은 매수하지 않음
                        logging.info(f"이미 보유 중인 코인: {ticker}, 매수 건너뜁니다.")
                        continue

                    krw_balance = float(upbit.get_balance("KRW"))
                    if krw_balance < 5500:  # 잔고 확인
                        logging.info("잔고가 부족하여 추가 매수를 포기합니다.")
                        break

                    logging.info(f"매수할 코인: {ticker}")
                    buy_result = upbit.buy_market_order(ticker, 5500)
                    logging.info(f"매수 완료 - 티커: {ticker}, 결과: {buy_result}")
            else:
                logging.info("매수 없음")

            # 매도 로직
            for balance in upbit.get_balances():
                if isinstance(balance, dict) and 'currency' in balance and balance['currency'] != 'KRW':
                    ticker = f"KRW-{balance['currency']}"
                    balance_amount = float(balance['balance'])
                    avg_buy_price = float(balance['avg_buy_price'])

                    if balance_amount < 0.0001 or avg_buy_price == 0:
                        continue

                    df = pyupbit.get_ohlcv(ticker, "minute5")
                    if df is not None:
                        current_price = pyupbit.get_current_price(ticker)

                        # 각 코인별 시장 상황 판단
                        market_trend = determine_market_trend(ticker)
                        # logging.info(f"{ticker}의 현재 시장 상황: {market_trend}")

                        # 매도 조건 - 시장 상황에 따라 다르게 설정
                        if market_trend == "bull":
                            if current_price >= avg_buy_price * 1.02:  # 2% 수익 시 매도
                                logging.info(f"{ticker} 매도 실행 - 상승장 목표 수익 달성")
                                sell_result = upbit.sell_market_order(ticker, balance['balance'])
                                logging.info(f"매도 완료 - 티커: {ticker}, 결과: {sell_result}")
                            elif current_price <= avg_buy_price * 0.98:  # 2% 손절 시 매도
                                logging.info(f"{ticker} 매도 실행 - 상승장 손절")
                                sell_result = upbit.sell_market_order(ticker, balance['balance'])
                                logging.info(f"매도 완료 - 티커: {ticker}, 결과: {sell_result}")

                        elif market_trend == "bear":
                            if current_price <= avg_buy_price * 0.97:  # 3% 손절 시 매도
                                logging.info(f"{ticker} 매도 실행 - 하락장 손절")
                                sell_result = upbit.sell_market_order(ticker, balance['balance'])
                                logging.info(f"매도 완료 - 티커: {ticker}, 결과: {sell_result}")
                            elif current_price >= avg_buy_price * 1.01:  # 1% 수익 시 매도
                                logging.info(f"{ticker} 매도 실행 - 하락장 목표 수익 달성")
                                sell_result = upbit.sell_market_order(ticker, balance['balance'])
                                logging.info(f"매도 완료 - 티커: {ticker}, 결과: {sell_result}")

                        elif market_trend == "sideways":
                            if current_price >= avg_buy_price * 1.015:  # 1.5% 수익 시 매도
                                logging.info(f"{ticker} 매도 실행 - 횡보장 목표 수익 달성")
                                sell_result = upbit.sell_market_order(ticker, balance['balance'])
                                logging.info(f"매도 완료 - 티커: {ticker}, 결과: {sell_result}")
                            elif current_price <= avg_buy_price * 0.985:  # 1.5% 손절 시 매도
                                logging.info(f"{ticker} 매도 실행 - 횡보장 손절")
                                sell_result = upbit.sell_market_order(ticker, balance['balance'])
                                logging.info(f"매도 완료 - 티커: {ticker}, 결과: {sell_result}")

                    time.sleep(2)

            time.sleep(1)

        except Exception as e:
            logging.error(f"매수 및 매도 판단 중 오류 발생: {e}")

# 메인 루프에서 스레드 동작 확인
def main():
    buy_thread = threading.Thread(target=trading_strategy, daemon=True)
    buy_thread.start()

    while True:
        if not buy_thread.is_alive():
            logging.error("매수 스레드가 중지됨, 재시작합니다.")
            buy_thread = threading.Thread(target=trading_strategy, daemon=True)
            buy_thread.start()
        time.sleep(5)

if __name__ == "__main__":
    main()
