import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from logging.handlers import TimedRotatingFileHandler

import pyupbit
from dotenv import load_dotenv

# .env 파일에서 환경 변수 로드
load_dotenv()

# 로그 파일 저장 경로 설정
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_file = os.path.join(log_dir, "trading_log.log")

# 로깅 설정: 로그를 기록할 방식과 저장 위치 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 로그 파일은 매일 자정에 새로운 파일로 로테이션되도록 설정
handler = TimedRotatingFileHandler(log_file, when="midnight", interval=1, encoding="utf-8")
handler.suffix = "%Y-%m-%d"  # 날짜 형식 설정
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# 콘솔에도 로그를 출력하기 위한 설정
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


# RSI 계산 함수: 특정 주기의 가격 차이를 이용하여 상승/하락 강도를 계산
def calculate_rsi(data, period=14):
    delta = data['close'].diff()  # 가격 차이 계산
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()  # 상승 강도
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()  # 하락 강도
    rs = gain / loss  # 상승/하락 강도의 비율
    rsi = 100 - (100 / (1 + rs))  # RSI 계산
    return rsi.iloc[-1]  # 마지막 RSI 값 반환


# 매수 조건 판단 함수: RSI 값이 특정 범위에 있을 때 매수 신호
def is_buy_condition(data):
    recent_rsi = calculate_rsi(data)
    return 40 <= recent_rsi <= 60  # RSI가 40과 60 사이일 때 매수


# 매도 조건 판단 함수: 현재 가격과 평균 매입 가격을 비교하여 매도 신호
def is_sell_condition(current_price, avg_buy_price, trend):
    if trend == "bull":
        if current_price >= avg_buy_price * 1.005:  # 0.5% 이상 상승 시 수익 실현
            return "profit"
        elif current_price <= avg_buy_price * 0.995:  # 0.5% 이하 하락 시 손실
            return "loss"
    elif trend == "bear":
        if current_price >= avg_buy_price * 1.002:  # 0.2% 이상 상승 시 수익 실현
            return "profit"
        elif current_price <= avg_buy_price * 0.99:  # 1% 이하 하락 시 손실
            return "loss"
    elif trend == "sideways":
        if current_price >= avg_buy_price * 1.003:  # 0.3% 이상 상승 시 수익 실현
            return "profit"
        elif current_price <= avg_buy_price * 0.997:  # 0.3% 이하 하락 시 손실
            return "loss"
    return None  # 매도 조건이 아니면 None 반환


# 시장 트렌드 판단 함수: 이동 평균선으로 시장이 상승, 하락, 또는 횡보하는지 판단
def determine_market_trend(ticker):
    data = pyupbit.get_ohlcv(ticker, interval="minute1")  # 1분 간격 OHLCV 데이터 가져오기
    if data is None or data.empty:
        return "sideways"  # 데이터가 없으면 횡보로 간주
    short_ma = data['close'].rolling(window=12).mean().iloc[-1]  # 12분 간격 이동 평균
    long_ma = data['close'].rolling(window=26).mean().iloc[-1]  # 26분 간격 이동 평균
    if short_ma > long_ma:
        return "bull"  # 단기 MA가 장기 MA보다 크면 상승 추세
    elif short_ma < long_ma:
        return "bear"  # 단기 MA가 장기 MA보다 작으면 하락 추세
    else:
        return "sideways"  # MA가 같으면 횡보 추세


# 보유하고 있는 티커 목록 반환 함수
def get_owned_tickers():
    balances = upbit.get_balances()  # 보유한 자산 조회
    owned_tickers = set()  # 보유한 티커를 저장할 세트
    for balance in balances:
        if isinstance(balance, dict) and 'currency' in balance and balance['currency'] != 'KRW':
            ticker = f"KRW-{balance['currency']}"  # 원화로 거래되는 티커 찾기
            owned_tickers.add(ticker)  # 보유 중인 티커 추가
    return owned_tickers


# 매수 전략 함수: 매수 조건을 만족하는 코인을 찾아 매수
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

            krw_balance = float(upbit.get_balance("KRW"))  # KRW 잔고 조회
            if krw_balance < 5500:
                # logging.info("잔고 부족으로 매수 로직 생략")
                time.sleep(11)
                continue

            owned_tickers = get_owned_tickers()  # 보유한 티커 조회
            buy_candidates = []  # 매수 후보 리스트

            # 멀티스레드를 사용하여 각 티커의 매수 조건을 판단
            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_ticker = {executor.submit(pyupbit.get_ohlcv, ticker, "minute1"): ticker for ticker in
                                    valid_tickers}
                for future in as_completed(future_to_ticker):
                    ticker = future_to_ticker[future]
                    try:
                        df = future.result()
                        if df is None or ticker in owned_tickers:
                            continue
                        if is_buy_condition(df):  # 매수 조건을 만족하는 경우
                            buy_candidates.append(ticker)
                            # logging.info(f"{ticker} - 매수 조건 충족")
                    except Exception as e:
                        logging.warning(f"{ticker} 데이터 오류: {e}")

            if buy_candidates:
                for ticker in buy_candidates:
                    owned_tickers = get_owned_tickers()
                    if len(owned_tickers) >= 50:
                        # logging.info("최대 보유 코인 수 도달")
                        break
                    if ticker in owned_tickers:
                        # logging.info(f"이미 보유 중: {ticker}, 매수 건너뜀")
                        continue
                    krw_balance = float(upbit.get_balance("KRW"))
                    if krw_balance < 5500:
                        # logging.info(f"잔고 부족으로 {ticker} 매수 포기")
                        break
                    logging.info(f"매수할 코인: {ticker}")
                    buy_result = upbit.buy_market_order(ticker, 5500)  # 시장가로 매수 주문 실행
                    # logging.info(f"매수 완료 - 티커: {ticker}, 결과: {buy_result}")
                    logging.info(f"매수 완료 - 티커: {ticker}")
            time.sleep(11)

        except Exception as e:
            logging.error(f"매수 전략 중 오류 발생: {e}")


# 매도 전략 함수: 매도 조건을 판단하여 보유 코인을 매도
def sell_strategy():
    while True:
        try:
            for balance in upbit.get_balances():
                if isinstance(balance, dict) and 'currency' in balance and balance['currency'] != 'KRW':
                    ticker = f"KRW-{balance['currency']}"  # 보유 중인 코인 티커
                    balance_amount = float(balance['balance'])  # 보유 수량
                    avg_buy_price = float(balance['avg_buy_price'])  # 평균 매수 가격
                    if balance_amount < 0.0001 or avg_buy_price == 0:
                        continue  # 보유 수량이 너무 적거나 평균 매수 가격이 없는 경우 건너뜀

                    df = pyupbit.get_ohlcv(ticker, "minute1")  # 1분 간격 OHLCV 데이터 가져오기
                    if df is not None:
                        current_price = pyupbit.get_current_price(ticker)  # 현재 가격 조회
                        trend = determine_market_trend(ticker)  # 시장 트렌드 판단
                        sell_condition = is_sell_condition(current_price, avg_buy_price, trend)  # 매도 조건 판단
                        if sell_condition:
                            # 수익 계산
                            profit = (current_price - avg_buy_price) * balance_amount  # 수익 금액 계산
                            profit_percentage = ((current_price - avg_buy_price) / avg_buy_price) * 100  # 수익률 계산

                            # 매도 실행
                            if sell_condition == "profit":
                                logging.info(f"{ticker} 매도 실행 - 트렌드: {trend}, 수익 실현")
                            elif sell_condition == "loss":
                                logging.info(f"{ticker} 매도 실행 - 트렌드: {trend}, 손절")
                            sell_result = upbit.sell_market_order(ticker, balance['balance'])  # 시장가로 매도 주문 실행
                            # logging.info(f"매도 완료 - 티커: {ticker}, 결과: {sell_result}")
                            logging.info(f"매도 완료 - 티커: {ticker}, 결과: {profit:.2f}원, 수익률: {profit_percentage:.2f}%")
                    time.sleep(11)

            time.sleep(11)

        except Exception as e:
            logging.error(f"매도 전략 중 오류 발생: {e}")


# 메인 함수: 매수와 매도 전략을 별도의 스레드로 실행
def main():
    buy_thread = threading.Thread(target=buy_strategy, daemon=True)
    sell_thread = threading.Thread(target=sell_strategy, daemon=True)
    buy_thread.start()
    sell_thread.start()

    while True:
        if not buy_thread.is_alive():  # 매수 스레드가 종료되면 재시작
            logging.error("매수 스레드가 중지됨, 재시작합니다.")
            buy_thread = threading.Thread(target=buy_strategy, daemon=True)
            buy_thread.start()
        if not sell_thread.is_alive():  # 매도 스레드가 종료되면 재시작
            logging.error("매도 스레드가 중지됨, 재시작합니다.")
            sell_thread = threading.Thread(target=sell_strategy, daemon=True)
            sell_thread.start()
        time.sleep(3)


if __name__ == "__main__":
    main()
