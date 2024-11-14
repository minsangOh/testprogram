import re

# 로그 파일 경로
log_file_path = "logs/trading_log.log"

# 파일에서 로그 데이터 읽어오기
with open(log_file_path, "r", encoding="utf-8") as file:
    log_data = file.read()

# 정규표현식을 사용하여 "결과"에 해당하는 금액 추출
result_pattern = re.compile(r"결과: ([\-]?\d+\.\d+)원")
results = result_pattern.findall(log_data)
# 문자열로 추출된 금액을 float으로 변환하고 합계 계산
total_result = sum(float(result) for result in results)

print(f"총 결과: {total_result:.2f} 원")
