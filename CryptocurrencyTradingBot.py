import pyupbit
import random
import threading
import logging
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

# .env 파일에서 환경 변수 로드
load_dotenv()

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 환경 변수에서 API 키 읽기
access_key = os.getenv("UPBIT_ACCESS_KEY")
secret_key = os.getenv("UPBIT_SECRET_KEY")

# API 키가 없는 경우 예외 처리
if not access_key or not secret_key:
    raise ValueError("API 키가 설정되지 않았습니다. .env 파일을 확인하세요.")

# 업비트 API 객체 생성
upbit = pyupbit.Upbit(access_key, secret_key)

# 유효한 티커 리스트를 캐시로 저장
valid_tickers = pyupbit.get_tickers(fiat="KRW")
logging.info(f"유효한 티커 리스트: {valid_tickers}")


# 유효한 티커인지 확인하는 함수
def is_valid_ticker(ticker):
    return ticker in valid_tickers


# 메시지가 출력되었는지를 추적하는 변수
message_printed = False

# 매수 실행 여부를 추적하는 변수
buy_executed = False

# 마지막으로 "매도 없음" 메시지가 출력된 시간을 저장
last_no_sell_message_time = 0

# 최대 재시도 횟수
MAX_RETRIES = 5

# 매도 체결 여부를 추적하는 플래그
sell_executed = threading.Event()


# 데이터를 가져오는 함수에 재시도 로직 추가
def get_ohlcv_with_retry(ticker, interval, retries=MAX_RETRIES):
    for attempt in range(retries):
        try:
            df = pyupbit.get_ohlcv(ticker, interval=interval)
            if df is not None:
                return df
        except Exception as e:
            logging.warning(f"{ticker}에 대한 데이터를 가져오는 중 오류 발생: {e}")
        time.sleep(1)  # 재시도하기 전에 잠시 대기
    logging.warning(f"{ticker}에 대한 데이터를 {retries}회 시도 후에도 가져올 수 없습니다.")
    return None


# 매수 판단 함수
def check_and_buy():
    global message_printed
    global buy_executed

    while True:
        try:
            start_time = time.time()  # 매수 판단 시작 시간 기록

            # 계좌 잔액 확인
            krw_balance = float(upbit.get_balance("KRW"))
            logging.info(f"현재 원화 잔액: {krw_balance} KRW")
            message_printed = True

            # 잔고가 5500원보다 적으면 매수하지 않고 매도가 체결될 때까지 대기
            if krw_balance < 5500:
                logging.info("잔고가 부족하여 매수를 대기합니다.")
                sell_executed.wait()  # 매도 체결될 때까지 대기
                sell_executed.clear()  # 매도 체결 후 플래그 초기화
                continue

            # 거래량 상위 35개 코인 선택
            tickers = pyupbit.get_tickers(fiat="KRW")
            volumes = {}

            with ThreadPoolExecutor(max_workers=10) as executor:
                future_to_ticker = {executor.submit(get_ohlcv_with_retry, ticker, "day1"): ticker for ticker in tickers}
                for future in as_completed(future_to_ticker):
                    ticker = future_to_ticker[future]
                    try:
                        df = future.result()
                        if df is not None:
                            volumes[ticker] = df.iloc[-1]['volume']
                        else:
                            logging.warning(f"{ticker}에 대한 데이터를 가져올 수 없습니다.")
                            message_printed = True
                    except Exception as e:
                        logging.warning(f"{ticker}에 대한 데이터를 가져오는 중 오류 발생: {e}")

            # 상위 35개 티커 선택
            top_35_tickers = sorted(volumes, key=volumes.get, reverse=True)[:35]

            # 5분 내 1% 이상 상승한 코인 필터링
            rising_1_percent = []
            with ThreadPoolExecutor(max_workers=10) as executor:
                future_to_ticker = {executor.submit(get_ohlcv_with_retry, ticker, "minute5"): ticker for ticker in
                                    top_35_tickers}
                for future in as_completed(future_to_ticker):
                    ticker = future_to_ticker[future]
                    try:
                        df = future.result()
                        if df is not None:
                            if (df.iloc[-1]['close'] - df.iloc[-2]['close']) / df.iloc[-2]['close'] >= 0.01:
                                rising_1_percent.append(ticker)
                        else:
                            logging.warning(f"{ticker}에 대한 데이터를 가져올 수 없습니다.")
                            message_printed = True
                    except Exception as e:
                        logging.warning(f"{ticker}에 대한 데이터를 가져오는 중 오류 발생: {e}")

            # 90초 내 0.35% 이상 상승한 코인 필터링
            rising_0_35_percent = []
            with ThreadPoolExecutor(max_workers=10) as executor:
                future_to_ticker = {executor.submit(get_ohlcv_with_retry, ticker, "second90"): ticker for ticker in
                                    rising_1_percent}
                for future in as_completed(future_to_ticker):
                    ticker = future_to_ticker[future]
                    try:
                        df = future.result()
                        if df is not None:
                            if (df.iloc[-1]['close'] - df.iloc[-2]['close']) / df.iloc[-2]['close'] >= 0.0035:
                                rising_0_35_percent.append(ticker)
                        else:
                            logging.warning(f"{ticker}에 대한 데이터를 가져올 수 없습니다.")
                            message_printed = True
                    except Exception as e:
                        logging.warning(f"{ticker}에 대한 데이터를 가져오는 중 오류 발생: {e}")

            # 1초 내 0.075% 이상 하락한 코인 제외
            filtered_tickers = []
            with ThreadPoolExecutor(max_workers=10) as executor:
                future_to_ticker = {executor.submit(get_ohlcv_with_retry, ticker, "second1"): ticker for ticker in
                                    rising_0_35_percent}
                for future in as_completed(future_to_ticker):
                    ticker = future_to_ticker[future]
                    try:
                        df = future.result()
                        if df is not None:
                            if (df.iloc[-1]['close'] - df.iloc[-2]['close']) / df.iloc[-2]['close'] > -0.00075:
                                filtered_tickers.append(ticker)
                        else:
                            logging.warning(f"{ticker}에 대한 데이터를 가져올 수 없습니다.")
                            message_printed = True
                    except Exception as e:
                        logging.warning(f"{ticker}에 대한 데이터를 가져오는 중 오류 발생: {e}")

            # 유효한 티커만 남기기
            filtered_tickers = [ticker for ticker in filtered_tickers if is_valid_ticker(ticker)]

            # 무작위 코인 매수
            if filtered_tickers:
                selected_ticker = random.choice(filtered_tickers)
                logging.info(f"선택된 코인: {selected_ticker}")
                buy_result = upbit.buy_market_order(selected_ticker, 5500)  # 고정 금액 5500원으로 매수
                logging.info(f"매수 완료 - 티커: {selected_ticker}, 결과: {buy_result}")
                message_printed = True
                buy_executed = True  # 매수 실행 여부를 True로 설정
            else:
                logging.info("매수 없음")
                message_printed = True

            # 매수 판단 종료 시간 기록 및 소요 시간 출력
            end_time = time.time()
            elapsed_time = end_time - start_time
            logging.info(f"매수 판단 소요 시간: {elapsed_time:.2f} 초")

        except Exception as e:
            logging.error(f"매수 판단 중 오류 발생: {e}")
            message_printed = True


# 1.75% 이상 상승 시 1.5%로 매도하는 함수
def check_and_sell_increase():
    global message_printed
    global last_no_sell_message_time
    while True:
        try:
            # 계좌 보유 자산 확인
            balances = upbit.get_balances()
            any_sell_executed = False  # 매도 실행 여부를 추적

            # 판매 가능한 코인이 있는지 확인
            for balance in balances:
                if isinstance(balance, dict) and 'currency' in balance:
                    ticker = f"KRW-{balance['currency']}"
                    balance_amount = float(balance['balance'])
                    avg_buy_price = float(balance['avg_buy_price'])

                    # 최소 잔고 기준 설정 (예: 0.0001 이상인 경우)
                    if balance_amount < 0.0001:
                        continue

                    # 평균 매수 가격이 0인 경우 매도하지 않음
                    if avg_buy_price == 0:
                        continue

                    # 티커 유효성 검사
                    if not is_valid_ticker(ticker):
                        continue

                    try:
                        current_price = pyupbit.get_current_price(ticker)

                        if current_price is not None:
                            # 현재 가격이 매수가 대비 1.75% 상승했는지 확인
                            if current_price >= avg_buy_price * 1.0175:
                                # 매도 주문 설정: 매수가격의 1.015배
                                target_sell_price = avg_buy_price * 1.015
                                # 가격을 소수점 이하 2자리까지로 포맷팅
                                target_sell_price = round(target_sell_price, 2)
                                logging.info(f"{ticker} 매도 주문 실행! 목표가: {target_sell_price}")
                                sell_result = upbit.sell_limit_order(ticker, target_sell_price, balance['balance'])
                                logging.info(f"매도 주문 완료 - 티커: {ticker}, 결과: {sell_result}")
                                message_printed = True
                                any_sell_executed = True
                                sell_executed.set()  # 매도 체결 플래그 설정
                    except pyupbit.errors.UpbitError as e:
                        logging.error(f"가격을 가져오는 중 오류 발생: {e}")
                        message_printed = True
                    except Exception as e:
                        logging.error(f"일반 오류 발생: {e}")
                        message_printed = True

            # 매도가 없는 경우, 마지막 "매도 없음" 메시지 출력 시점에서 30초 지난 후에만 메시지 출력
            if not any_sell_executed:
                current_time = time.time()
                if current_time - last_no_sell_message_time > 30:
                    logging.info("매도 없음")
                    last_no_sell_message_time = current_time

            # 매도 판단 후 1초 대기
            time.sleep(1)

        except Exception as e:
            logging.error(f"매도 판단 중 오류 발생: {e}")
            message_printed = True


# 2.75% 이상 하락 시 매도하는 함수
def check_and_sell_decrease():
    global message_printed
    global last_no_sell_message_time
    while True:
        try:
            # 계좌 보유 자산 확인
            balances = upbit.get_balances()
            any_sell_executed = False  # 매도 실행 여부를 추적

            # 판매 가능한 코인이 있는지 확인
            for balance in balances:
                if isinstance(balance, dict) and 'currency' in balance:
                    ticker = f"KRW-{balance['currency']}"
                    balance_amount = float(balance['balance'])
                    avg_buy_price = float(balance['avg_buy_price'])

                    # 최소 잔고 기준 설정 (예: 0.0001 이상인 경우)
                    if balance_amount < 0.0001:
                        continue

                    # 평균 매수 가격이 0인 경우 매도하지 않음
                    if avg_buy_price == 0:
                        continue

                    # 티커 유효성 검사
                    if not is_valid_ticker(ticker):
                        continue

                    try:
                        current_price = pyupbit.get_current_price(ticker)

                        if current_price is not None:
                            # 2.75% 이상 하락 시 매도
                            if current_price <= avg_buy_price * 0.9725:
                                logging.info(f"{ticker} 매도 실행! (2.75% 이상 하락)")
                                sell_result = upbit.sell_market_order(ticker, balance['balance'])
                                logging.info(f"매도 완료 - 티커: {ticker}, 결과: {sell_result}")
                                message_printed = True
                                any_sell_executed = True
                                sell_executed.set()  # 매도 체결 플래그 설정
                    except pyupbit.errors.UpbitError as e:
                        logging.error(f"가격을 가져오는 중 오류 발생: {e}")
                        message_printed = True
                    except Exception as e:
                        logging.error(f"일반 오류 발생: {e}")
                        message_printed = True

            # 매도가 없는 경우, 마지막 "매도 없음" 메시지 출력 시점에서 30초가 지난 후에만 메시지 출력
            if not any_sell_executed:
                current_time = time.time()
                if (current_time - last_no_sell_message_time) > 30:
                    logging.info("매도 없음")
                    last_no_sell_message_time = current_time

            # 매도 판단 후 1초 대기
            time.sleep(1)

        except Exception as e:
            logging.error(f"매도 판단 중 오류 발생: {e}")
            message_printed = True


# 매수 판단을 별도의 스레드에서 실행
buy_thread = threading.Thread(target=check_and_buy)
buy_thread.start()

# 매도 조건 체크를 별도의 스레드에서 실행
sell_increase_thread = threading.Thread(target=check_and_sell_increase)
sell_increase_thread.start()

sell_decrease_thread = threading.Thread(target=check_and_sell_decrease)
sell_decrease_thread.start()

# 메인 프로그램이 종료되지 않도록 유지
while True:
    time.sleep(1)
    if message_printed:
        logging.info("Trading running")
        message_printed = False  # 메시지 출력 후 다시 False로 초기화
