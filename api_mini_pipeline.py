# =================================================================================
# [프로그램 개요]
# - 작성 목적: 3개 외부 API 비동기 수집, Pydantic v2 검증, 성능 비교, 테스트를 단일 파일에서 수행
# - 실행 방법 (메인): python pipeline_strict_type.py
# - 실행 방법 (테스트): pytest pipeline_strict_type.py -v
# 
# 작성일 : 2026-07-15
# 작성자 : 이현우
# =================================================================================

import asyncio
import os
import time

import httpx
import pandas as pd
import pytest
from pydantic import BaseModel, Field, ValidationError, field_validator

# ==============================================================================
# 스키마 검증 (Pydantic v2 모델 정의)
# ==============================================================================

class HourlyWeather(BaseModel):
    """Open-Meteo의 시간대별 날씨 데이터를 검증하는 스키마"""
    time: list[str] = Field(..., description="시간대 배열 (ISO 8601 포맷)")
    temperature_2m: list[float] = Field(..., description="2m 상공 기온 배열")
    precipitation_probability: list[int] = Field(..., description="강수 확률 배열 (%)")

    @field_validator("precipitation_probability")
    @classmethod
    def validate_probability(cls, values: list[int]) -> list[int]:
        """강수 확률이 0% ~ 100% 사이의 유효한 범위를 가지는지 검증합니다."""
        if not all(0 <= p <= 100 for p in values):
            raise ValueError("강수 확률은 반드시 0%에서 100% 사이의 값이어야 합니다.")
        return values

class WeatherModel(BaseModel):
    """Open-Meteo 최상위 응답 검증 스키마"""
    latitude: float = Field(..., description="요청 위도")
    longitude: float = Field(..., description="요청 경도")
    hourly: HourlyWeather = Field(..., description="시간대별 날씨 객체")

class CountryModel(BaseModel):
    """Nager.Date API (국가 정보) 응답 검증 스키마"""
    commonName: str = Field(..., description="국가 일반 명칭")
    region: str = Field(..., description="소속 대륙")

class IpModel(BaseModel):
    """ip-api 응답 검증 스키마"""
    status: str = Field(..., description="API 응답 상태")
    country: str = Field(..., description="IP 소속 국가")
    city: str = Field(..., description="IP 소속 도시")
    query: str = Field(..., description="조회한 IP 주소")

    @field_validator("status")
    @classmethod
    def validate_status(cls, value: str) -> str:
        """API 호출 상태가 성공(success)인지 검증합니다."""
        if value != "success":
            raise ValueError(f"IP 정보 조회에 실패했습니다. (상태: {value})")
        return value

# ==============================================================================
# [채점 기준 1] 비동기 수집 (asyncio + httpx)
# ==============================================================================

async def fetch_api(client: httpx.AsyncClient, url: str, name: str) -> dict:
    """
    단일 API에 비동기 GET 요청을 보내고 파싱된 JSON 객체(dict)를 반환합니다.
    """
    try:
        response = await client.get(url, timeout=10.0)
        response.raise_for_status()
        print(f"[수집 성공] {name} API 데이터 로드 완료")
        return response.json()
    except httpx.HTTPError as e:
        print(f"[수집 실패] {name} API 에러 발생: {e}")
        raise

async def collect_data() -> tuple[dict, dict, dict]:
    """
    3개의 API를 asyncio.gather()를 활용하여 동시에 수집합니다.
    (모든 응답이 dict 타입으로 안전하게 반환됩니다.)
    """
    urls = {
        "weather": "https://api.open-meteo.com/v1/forecast?latitude=37.5665&longitude=126.9780&hourly=temperature_2m,precipitation_probability&forecast_days=3&timezone=Asia/Seoul",
        "country": "https://date.nager.at/api/v3/CountryInfo/KR", # RestCountries 대체 API 적용
        "ip": "http://ip-api.com/json/8.8.8.8"
    }

    async with httpx.AsyncClient(follow_redirects=True) as client:
        tasks = [
            fetch_api(client, urls["weather"], "Open-Meteo"),
            fetch_api(client, urls["country"], "Nager.Date(Country)"),
            fetch_api(client, urls["ip"], "ip-api")
        ]
        
        # 반환되는 결과들을 튜플로 구성 (모두 dict 형태 보장)
        weather_res, country_res, ip_res = await asyncio.gather(*tasks)
        return weather_res, country_res, ip_res

# ==============================================================================
# 저장 및 성능 비교 (CSV vs Parquet)
# ==============================================================================

def compare_storage_performance(df: pd.DataFrame) -> None:
    """
    데이터프레임을 CSV와 Parquet 형식으로 각각 저장/읽기하여 속도와 파일 크기를 측정 및 비교합니다.
    """
    csv_path = "pipeline_data.csv"
    parquet_path = "pipeline_data.parquet"

    # 1. CSV 포맷 성능 측정
    start_time = time.perf_counter()
    df.to_csv(csv_path, index=False)
    csv_write_time = time.perf_counter() - start_time

    start_time = time.perf_counter()
    _ = pd.read_csv(csv_path)
    csv_read_time = time.perf_counter() - start_time
    csv_size = os.path.getsize(csv_path) / 1024  # KB로 변환

    # 2. Parquet 포맷 성능 측정
    start_time = time.perf_counter()
    df.to_parquet(parquet_path, engine="pyarrow", index=False)
    parquet_write_time = time.perf_counter() - start_time

    start_time = time.perf_counter()
    _ = pd.read_parquet(parquet_path, engine="pyarrow")
    parquet_read_time = time.perf_counter() - start_time
    parquet_size = os.path.getsize(parquet_path) / 1024  # KB로 변환

    # 3. 콘솔에 비교 결과 표출
    print("\n[저장 매체 성능 비교 결과]")
    print(f"| {'포맷':<8} | {'쓰기 속도(초)':<15} | {'읽기 속도(초)':<15} | {'파일 크기(KB)':<15} |")
    print("-" * 65)
    print(f"| {'CSV':<8} | {csv_write_time:<15.6f} | {csv_read_time:<15.6f} | {csv_size:<15.2f} |")
    print(f"| {'Parquet':<8} | {parquet_write_time:<15.6f} | {parquet_read_time:<15.6f} | {parquet_size:<15.2f} |")

# ==============================================================================
# pytest 단위 테스트 로직 (파일 내부에 포함)
# ==============================================================================

def test_weather_schema_valid() -> None:
    """[정상 케이스 테스트] 올바른 형태의 날씨 데이터가 유입될 때 예외가 발생하지 않는지 검증합니다."""
    valid_data = {
        "latitude": 37.5,
        "longitude": 126.9,
        "hourly": {
            "time": ["2026-07-15T12:00", "2026-07-15T13:00"],
            "temperature_2m": [25.5, 26.0],
            "precipitation_probability": [0, 50]
        }
    }
    model = WeatherModel.model_validate(valid_data)
    assert model.latitude == 37.5
    assert len(model.hourly.time) == 2

def test_weather_schema_invalid_probability() -> None:
    """[예외 케이스 테스트] 강수 확률이 100을 초과할 때 ValidationError 예외가 터지는지 검증합니다."""
    invalid_data = {
        "latitude": 37.5,
        "longitude": 126.9,
        "hourly": {
            "time": ["2026-07-15T12:00"],
            "temperature_2m": [25.5],
            "precipitation_probability": [150]
        }
    }
    with pytest.raises(ValidationError):
        WeatherModel.model_validate(invalid_data)

def test_ip_schema_invalid_status() -> None:
    """[예외 케이스 테스트] IP API의 응답 상태(status)가 success가 아닐 때 ValidationError가 발생하는지 검증합니다."""
    invalid_data = {
        "status": "fail",
        "country": "Unknown",
        "city": "Unknown",
        "query": "0.0.0.0"
    }
    with pytest.raises(ValidationError):
        IpModel.model_validate(invalid_data)

# ==============================================================================
# 메인 실행 흐름
# ==============================================================================

def main() -> None:
    """데이터 수집, 검증, 성능 비교를 순차적으로 통제하는 메인 함수입니다."""
    print("--- 🚀 통합 데이터 수집 파이프라인 시작 ---")
    
    # 1. 비동기 데이터 동시 수집
    weather_raw, country_raw, ip_raw = asyncio.run(collect_data())

    # 2. Pydantic v2 스키마 검증 및 예외 처리
    try:
        weather_data = WeatherModel.model_validate(weather_raw)
        country_data = CountryModel.model_validate(country_raw)  # API 변경으로 인덱싱[0] 제거!
        ip_data = IpModel.model_validate(ip_raw)
        print("\n[검증 통과] 모든 API 데이터의 스키마 및 타입/범위 검증을 완료했습니다.")
    except ValidationError as e:
        print(f"\n[검증 실패] 데이터 스키마 오류가 발생했습니다:\n{e.json()}")
        return

    # 3. 검증 통과 데이터로 Pandas DataFrame 병합
    df = pd.DataFrame(weather_data.hourly.model_dump())
    df["req_ip"] = ip_data.query
    df["ip_city"] = ip_data.city
    df["country_name"] = country_data.commonName
    df["country_region"] = country_data.region

    # 4. 성능 비교 결과 출력
    compare_storage_performance(df)
    print("\n--- ✅ 파이프라인 실행 종료 ---")

if __name__ == "__main__":
    main()