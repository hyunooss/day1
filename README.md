# Async API Data Pipeline

`api_mini_pipeline.py`는 세 개의 외부 API에서 데이터를 **비동기로 동시 수집**하고, **Pydantic v2**로 응답 스키마를 검증한 뒤, CSV와 Parquet 저장 성능을 비교하는 파이프라인입니다.

## 주요 기능

- `asyncio`와 `httpx`로 날씨·국가·IP 정보를 병렬 수집
- Pydantic v2 모델과 필드 검증기로 데이터 타입 및 값 범위 확인
  - 강수 확률은 0~100% 범위인지 검증
  - IP 조회 상태가 `success`인지 검증
- 검증된 데이터를 Pandas DataFrame으로 통합
- CSV와 Parquet의 읽기·쓰기 시간 및 파일 크기 비교
- 정상·예외 상황을 다루는 pytest 단위 테스트 포함

## 데이터 소스

| 구분 | API | 수집 항목 |
| --- | --- | --- |
| 날씨 | Open-Meteo | 시간별 기온, 강수 확률 |
| 국가 | countries.dev | 국가명, 지역 |
| IP | ip-api | IP, 국가, 도시 |

## 빠른 시작

### 1. 설치

~~~bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
~~~

### 2. 실행

~~~bash
python api_mini_pipeline.py
~~~

실행 후 프로젝트 루트에 다음 결과 파일이 생성되며, 콘솔에는 두 형식의 저장·읽기 시간과 파일 크기가 출력됩니다.

- `pipeline_data.csv`
- `pipeline_data.parquet`

### 3. 테스트

~~~bash
pytest api_mini_pipeline.py -v
~~~

## 처리 흐름

~~~text
외부 API 3개 동시 호출
        ↓
Pydantic 스키마 및 값 검증
        ↓
Pandas DataFrame 통합
        ↓
CSV · Parquet 저장/읽기 성능 비교
~~~

## 기술 스택

Python · asyncio · httpx · Pydantic v2 · Pandas · PyArrow · pytest

## 프로젝트 구조

~~~text
.
├── api_mini_pipeline.py    # 수집·검증·성능 비교·테스트
├── requirements.txt        # Python 의존성
├── pipeline_data.csv       # 실행 결과 CSV
└── pipeline_data.parquet   # 실행 결과 Parquet
~~~
