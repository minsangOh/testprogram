import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyupbit
from dotenv import load_dotenv
from logging.handlers import TimedRotatingFileHandler

# .env 파일에서 환경 변수 로드 (API 키와 같은 비밀 정보)
load_dotenv()

# 로그 파일 저장 경로 설정
log_dir = "logs"
if not os.path.exists(log_dir):  # 로그 디렉토리가 없으면 생성
    os.makedirs(log_dir)

log_file = os.path.join(log_dir, "trading_log.log")

# 로깅 설정 (파일로 저장하고 콘솔에도 출력)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 자정마다 로그 파일을 회전시키는 핸들러
handler = TimedRotatingFileHandler(log_file, when="midnight", interval=1, encoding="utf-8")
handler.suffix = "%Y-%m-%d"
handler.setLevel(logging.INFO)

# 로그 출력 형식 설정
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# 콘솔에도 로그를 출력하도록 설정
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# 초기 로그 출력
logger.info("초단기 매매 전략 시작")

# .env 파일에서 API 키 로드
access_key = os.getenv("UPBIT_ACCESS_KEY")
secret_key = os.getenv("UPBIT_SECRET_KEY")
if not access_key or not secret_key:
    raise ValueError("API 키가 설정되지 않았습니다. .env 파일을 확인하세요.")

# Upbit API 객체 생성
upbit = pyupbit.Upbit(access_key, secret_key)

# 유효한 티커 리스트 가져오기 (KRW 마켓)
valid_tickers = pyupbit.get_tickers(fiat="KRW")
logging.info(f"유효한 티커 리스트: {valid_tickers}")


# RSI 계산 함수
def calculate_rsi(data, period=14):
    delta = data['close'].diff()  # 종가 차이 계산
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()  # 상승분 평균
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()  # 하락분 평균
    rs = gain / loss  # 상대강도 지수 계산
    rsi = 100 - (100 / (1 + rs))  # RSI 계산
    return rsi.iloc[-1]  # 가장 최근 RSI 반환


# 매수 조건 판단 함수
def is_buy_condition(data):
    recent_rsi = calculate_rsi(data)  # 최근 RSI 계산
    return 40 <= recent_rsi <= 60  # RSI가 40에서 60 사이일 때 매수 조건


# 매도 조건 판단 함수
def is_sell_condition(current_price, avg_buy_price, trend):
    if trend == "bull":  # 상승장일 때
        if current_price >= avg_buy_price * 1.005:
            return "profit"  # 0.5% 이상 수익 시 매도
        elif current_price <= avg_buy_price * 0.995:
            return "loss"  # 0.5% 이하 손실 시 매도
    elif trend == "bear":  # 하락장일 때
        if current_price <= avg_buy_price * 0.99:
            return "loss"  # 1% 이하 손실 시 매도
        elif current_price >= avg_buy_price * 1.002:
            return "profit"  # 0.2% 이상 수익 시 매도
    elif trend == "sideways":  # 횡보장일 때
        if current_price >= avg_buy_price * 1.003:
            return "profit"  # 0.3% 이상 수익 시 매도
        elif current_price <= avg_buy_price * 0.997:
            return "loss"  # 0.3% 이하 손실 시 매도
    return None  # 매도 조건이 없으면 None 반환


# 시장 트렌드 판단 함수
def determine_market_trend(ticker):
    data = pyupbit.get_ohlcv(ticker, interval="minute1")  # 1분 봉 데이터 가져오기
    if data is None or data.empty:
        return "sideways"  # 데이터가 없으면 횡보장으로 처리

    short_ma = data['close'].rolling(window=12).mean().iloc[-1]  # 12기간 단기 이동평균
    long_ma = data['close'].rolling(window=26).mean().iloc[-1]  # 26기간 장기 이동평균
    if short_ma > long_ma:
        return "bull"  # 단기 이동평균이 장기 이동평균보다 크면 상승장
    elif short_ma < long_ma:
        return "bear"  # 단기 이동평균이 장기 이동평균보다 작으면 하락장
    else:
        return "sideways"  # 두 이동평균이 같으면 횡보장


# 보유하고 있는 티커 목록 반환 함수
def get_owned_tickers():
    balances = upbit.get_balances()  # 보유 자산 가져오기
    owned_tickers = set()
    for balance in balances:
        if isinstance(balance, dict) and 'currency' in balance and balance['currency'] != 'KRW':
            ticker = f"KRW-{balance['currency']}"  # 보유한 코인 티커 확인
            owned_tickers.add(ticker)
    return owned_tickers


# 매수 전략 함수
def buy_strategy():
    while True:
        try:
            krw_balance = float(upbit.get_balance("KRW"))  # 잔고 확인
            if krw_balance < 5500:  # 잔고가 부족하면 매수 포기
                logging.info("잔고 부족으로 매수 포기")
                time.sleep(2)
                continue

            tickers = pyupbit.get_tickers(fiat="KRW")  # 모든 티커 가져오기
            owned_tickers = get_owned_tickers()  # 보유한 티커 가져오기
            buy_candidates = []  # 매수 후보 티커 리스트

            # 비동기식으로 각 티커에 대한 매수 조건 체크
            with ThreadPoolExecutor(max_workers=10) as executor:
                future_to_ticker = {executor.submit(pyupbit.get_ohlcv, ticker, "minute1"): ticker for ticker in tickers}
                for future in as_completed(future_to_ticker):
                    ticker = future_to_ticker[future]
                    try:
                        df = future.result()
                        if df is None or ticker in owned_tickers:
                            continue

                        # 매수 조건이 충족되는 경우 후보에 추가
                        if is_buy_condition(df):
                            buy_candidates.append(ticker)
                            logging.info(f"{ticker} - 매수 조건 충족")

                    except Exception as e:
                        logging.warning(f"{ticker} 데이터 오류: {e}")

            # 매수 후보가 있으면 매수 실행
            if buy_candidates:
                for ticker in buy_candidates:
                    owned_tickers = get_owned_tickers()
                    if len(owned_tickers) >= 13:  # 최대 보유 코인 수 초과 시 매수 중지
                        logging.info("최대 보유 코인 수 도달")
                        break

                    if ticker in owned_tickers:
                        logging.info(f"이미 보유 중: {ticker}, 매수 건너뜀")
                        continue

                    krw_balance = float(upbit.get_balance("KRW"))
                    if krw_balance < 5500:
                        logging.info("잔고 부족으로 추가 매수 포기")
                        break

                    # 매수 실행
                    logging.info(f"매수할 코인: {ticker}")
                    buy_result = upbit.buy_market_order(ticker, 5500)
                    logging.info(f"매수 완료 - 티커: {ticker}, 결과: {buy_result}")
            time.sleep(2)

        except Exception as e:
            logging.error(f"매수 전략 중 오류 발생: {e}")


# 매도 전략 함수
def sell_strategy():
    while True:
        try:
            for balance in upbit.get_balances():  # 보유 코인들에 대해 매도 전략 수행
                if isinstance(balance, dict) and 'currency' in balance and balance['currency'] != 'KRW':
                    ticker = f"KRW-{balance['currency']}"
                    balance_amount = float(balance['balance'])  # 보유 수량
                    avg_buy_price = float(balance['avg_buy_price'])  # 평균 매수 가격
                    if balance_amount < 0.0001 or avg_buy_price == 0:
                        continue  # 잔고가 너무 적거나 평균 매수 가격이 0이면 건너뜀

                    df = pyupbit.get_ohlcv(ticker, "minute1")  # 1분 봉 데이터 가져오기
                    if df is not None:
                        current_price = pyupbit.get_current_price(ticker)  # 현재 가격
                        trend = determine_market_trend(ticker)  # 시장 트렌드 판단

                        # 매도 조건 체크
                        sell_condition = is_sell_condition(current_price, avg_buy_price, trend)
                        if sell_condition:
                            # 매도 실행
                            if sell_condition == "profit":
                                logging.info(f"{ticker} 매도 실행 - 트렌드: {trend}, 수익 실현")
                            elif sell_condition == "loss":
                                logging.info(f"{ticker} 매도 실행 - 트렌드: {trend}, 손절")
                            sell_result = upbit.sell_market_order(ticker, balance['balance'])
                            logging.info(f"매도 완료 - 티커: {ticker}, 결과: {sell_result}")
                    time.sleep(2)

            time.sleep(2)

        except Exception as e:
            logging.error(f"매도 전략 중 오류 발생: {e}")


# 메인 함수
def main():
    # 매수와 매도를 각각 독립적인 스레드에서 실행
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
