import logging
import os
import threading
import time
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

logger.info("단기 매매 전략 시작")

# .env 파일에서 API 키 로드
access_key = os.getenv("UPBIT_ACCESS_KEY")
secret_key = os.getenv("UPBIT_SECRET_KEY")
if not access_key or not secret_key:
    raise ValueError("API 키가 설정되지 않았습니다. .env 파일을 확인하세요.")

# Upbit API 객체 생성
upbit = pyupbit.Upbit(access_key, secret_key)

# 유효한 티커 리스트
valid_tickers = pyupbit.get_tickers(fiat="KRW")

# 계산 상수
BUY_AMOUNT = 10000  # 매수 금액
FEE_RATE = 0.0005  # 수수료율


# RSI 계산 함수: 특정 주기의 가격 차이를 이용하여 상승/하락 강도를 계산
def calculate_rsi(ticker, period=14):
    delta = ticker['close'].diff()  # 가격 차이 계산
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()  # 상승 강도
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()  # 하락 강도
    rs = gain / loss  # 상승/하락 강도의 비율
    rsi = 100 - (100 / (1 + rs))  # RSI 계산
    return rsi.iloc[-1]  # 마지막 RSI 값 반환


# 시장 트렌드 판단 함수: 이동 평균선으로 시장이 상승, 하락, 또는 횡보하는지 판단
def determine_market_trend(ticker):
    data = pyupbit.get_ohlcv(ticker, interval="minute1")  # 1분 간격 OHLCV 데이터 가져오기
    if data is None or data.empty:
        return "sideways"  # 데이터가 없으면 횡보로 간주
    short_ma = data['close'].rolling(window=7).mean().iloc[-1]  # 7분 간격 이동 평균
    long_ma = data['close'].rolling(window=21).mean().iloc[-1]  # 21분 간격 이동 평균
    if short_ma > long_ma:
        return "bull"  # 단기 MA가 장기 MA보다 크면 상승 추세
    elif short_ma < long_ma:
        return "bear"  # 단기 MA가 장기 MA보다 작으면 하락 추세
    else:
        return "sideways"  # MA가 같으면 횡보 추세


# 매수 조건 판단 함수: RSI가 30~60 사이이고 상승 추세일 때 매수 신호
def is_buy_condition(ticker):
    market_trend = determine_market_trend(ticker)
    recent_rsi = calculate_rsi(ticker)
    if 30 <= recent_rsi <= 60 and market_trend == "bull":
        return True
    return False


# 매도 조건 판단 함수: 현재 가격과 평균 매입 가격을 비교하여 매도 신호
def is_sell_condition(current_price, avg_buy_price, trend):
    global FEE_RATE
    cp = current_price
    cost = avg_buy_price * (1 + FEE_RATE)  # 평균 매입 가격에 매수 수수료 포함
    if trend == "bull":
        if cp >= cost * 1.005:  # 0.5% 이상 상승 시 수익 실현
            return "profit"
        elif cp <= cost * 0.995:  # 0.5% 이하 하락 시 손실
            return "loss"
    elif trend == "bear":
        if cp >= cost * 1.002:  # 0.2% 이상 상승 시 수익 실현
            return "profit"
        elif cp <= cost * 0.99:  # 1% 이하 하락 시 손실
            return "loss"
    elif trend == "sideways":
        if cp >= cost * 1.003:  # 0.3% 이상 상승 시 수익 실현
            return "profit"
        elif cp <= cost * 0.997:  # 0.3% 이하 하락 시 손실
            return "loss"
    return None  # 매도 조건이 아니면 None 반환


# 매수 전략 함수: 매수 조건을 만족하는 코인을 찾아 매수
def buy_strategy():
    while True:
        try:
            # 시장에 등록된 티커를 갱신 (12시간마다 실행)
            global valid_tickers
            if int(time.time()) % (12 * 60 * 60) == 0:  # 12시간 간격
                valid_tickers = pyupbit.get_tickers(fiat="KRW")

            # 내 계좌 정보 가져오기
            balances = upbit.get_balances()
            owned_coins = {balance['currency'] for balance in balances if float(balance['balance']) > 0}
            if len(owned_coins) >= 35:
                time.sleep(5)
                continue

            # 유효한 티커에서 OHLCV 데이터를 가져와 매수 조건 확인
            for ticker in valid_tickers:
                if ticker.split("-")[1] in owned_coins:  # 이미 보유한 코인 건너뛰기
                    continue
                data = pyupbit.get_ohlcv(ticker, interval="minute1")
                if data is None or data.empty:
                    continue

                # 매수 조건 만족 시 매수
                if is_buy_condition(data):
                    response = upbit.buy_market_order(ticker, BUY_AMOUNT)
                    logger.info(f"매수 완료: {response}")
                    time.sleep(0.5)
        except Exception as e:
            logger.error(f"매수 전략 오류: {e}")
        time.sleep(1)


# 매도 전략 함수: 매도 조건을 판단하여 보유 코인을 매도
def sell_strategy():
    while True:
        try:
            # 내 계좌 정보 가져오기
            balances = upbit.get_balances()
            for balance in balances:
                ticker = f"KRW-{balance['currency']}"

                # 원화 또는 상장폐지, 보유량 0인 코인 건너뛰기
                if balance['currency'] == "KRW" or balance['currency'] == "LUNC" or balance['currency'] == "APENFT" or \
                        balance['currency'] == "LUNA2":
                    continue
                elif float(balance['balance']) <= 0:
                    continue

                # 현재 가격 및 평균 매수 가격 가져오기
                current_price = pyupbit.get_current_price(ticker)
                avg_buy_price = float(balance['avg_buy_price'])
                trend = determine_market_trend(ticker)

                # 매도 조건 확인
                sell_condition = is_sell_condition(current_price, avg_buy_price, trend)

                # 수익률 계산
                volume = float(balance['balance'])
                total_buy_cost = avg_buy_price * (1 + FEE_RATE) * volume  # 매수 비용
                total_sell_revenue = current_price * (1 - FEE_RATE) * volume  # 매도 수익
                net_profit = total_sell_revenue - total_buy_cost  # 순수익
                profit_percent = (net_profit / total_buy_cost) * 100  # 수익률

                # 매도 함수 실행
                upbit.sell_market_order(ticker, volume)

                # 분기 별 로그 기록
                if sell_condition == "profit":
                    logger.info(f"수익 실현 매도 완료: {ticker}, 순수익: {net_profit:.2f}원, 수익률: {profit_percent:.2f}%")
                elif sell_condition == "loss":
                    logger.info(f"손실 매도 완료: {ticker}, 순손실: {net_profit:.2f}원, 손실률: {profit_percent:.2f}%")

                time.sleep(0.5)

        except Exception as e:
            logger.error(f"매도 전략 오류: {e}")
        time.sleep(1)


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
