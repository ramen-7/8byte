from __future__ import annotations

import os
import json
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple

import requests
import psycopg2
from psycopg2.extras import execute_values
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from airflow import DAG
from airflow.operators.python import PythonOperator


class FetchError(Exception):
    pass


def _get_env(name: str, default: Optional[str] = None, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and (value is None or value == ""):
        raise ValueError(f"Missing required environment variable: {name}")
    return value  


def _build_db_conn_params() -> Dict[str, Any]:
    return {
        "host": _get_env("STOCK_DB_HOST", required=True),
        "port": int(_get_env("STOCK_DB_PORT", "5432")),
        "dbname": _get_env("STOCK_DB_NAME", required=True),
        "user": _get_env("STOCK_DB_USER", required=True),
        "password": _get_env("STOCK_DB_PASSWORD", required=True),
    }


def _parse_alpha_vantage_daily_json(symbol: str, payload: Dict[str, Any]) -> Tuple[str, list]:
    meta = payload.get("Meta Data") or {}
    series = payload.get("Time Series (Daily)") or {}
    if not series:
        raise FetchError("Alpha Vantage response missing 'Time Series (Daily)'")

    rows = []
    for date_str, values in series.items():
        try:
            ts = datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            continue
        row = (
            symbol,
            ts,
            _safe_float(values.get("1. open")),
            _safe_float(values.get("2. high")),
            _safe_float(values.get("3. low")),
            _safe_float(values.get("4. close")),
            _safe_int(values.get("5. volume")),
            json.dumps(meta),
        )
        rows.append(row)

    rows.sort(key=lambda r: r[1])
    return symbol, rows


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=30), reraise=True,
       retry=retry_if_exception_type((requests.RequestException, FetchError)))
def fetch_alpha_vantage_daily(symbol: str, api_key: str) -> Dict[str, Any]:
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": "compact",
        "apikey": api_key,
    }
    
    print(f"Making API call to Alpha Vantage for symbol: {symbol}")
    print(f"API Key present: {'Yes' if api_key else 'No'}")
    print(f"API Key length: {len(api_key) if api_key else 0}")
    print(f"URL: {url}")
    print(f"Params: {params}")
    
    resp = requests.get(url, params=params, timeout=30)
    print(f"Response status: {resp.status_code}")
    print(f"Response headers: {dict(resp.headers)}")
    
    if resp.status_code != 200:
        print(f"HTTP Error: {resp.text[:200]}")
        raise FetchError(f"HTTP {resp.status_code}: {resp.text[:200]}")
    
    data = resp.json()
    print(f"Response data keys: {list(data.keys())}")
    
    # Alpha Vantage sends rate limit notes or error messages in JSON
    if "Note" in data:
        print(f"Rate limit warning: {data['Note']}")
        # rate limit hit, sleep for 20 seconds
        time.sleep(20)
        raise FetchError("Alpha Vantage rate limited: Note present in payload")
    if "Information" in data:
        print(f"Information from API: {data['Information']}")
        time.sleep(20)
        raise FetchError("Alpha Vantage throttled or invalid request: Information present in payload")
    if "Error Message" in data:
        print(f"Alpha Vantage error: {data['Error Message']}")
        raise FetchError(f"Alpha Vantage error: {data['Error Message']}")

    print(f"Successfully fetched data for {symbol}")
    return data


def upsert_rows(rows: list) -> int:
    if not rows:
        print("No rows to insert")
        return 0
    
    print(f"inserting {len(rows)} rows to database")
    conn = None
    inserted = 0
    try:
        conn = psycopg2.connect(**_build_db_conn_params())
        conn.autocommit = False
        with conn.cursor() as cur:
            sql = (
                "INSERT INTO public.stock_prices (symbol, ts, open, high, low, close, volume, meta) "
                "VALUES %s "
                "ON CONFLICT (symbol, ts) DO UPDATE SET "
                "open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, "
                "close = EXCLUDED.close, volume = EXCLUDED.volume, meta = EXCLUDED.meta, updated_at = NOW()"
            )
            execute_values(cur, sql, rows, template=None, page_size=100)
        conn.commit()
        inserted = len(rows)
        print(f"Successfully inserted {inserted} rows")
    except Exception as ex:
        print(f"Database error: {ex}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()
    return inserted


def run_pipeline(symbol: Optional[str] = None) -> Dict[str, Any]:
    print(f"Starting pipeline for symbol: {symbol}")
    api_key = _get_env("ALPHAVANTAGE_API_KEY", required=True)
    symbol = symbol or _get_env("STOCK_SYMBOL", "MSFT")
    
    print(f"Fetching data for {symbol}")
    payload = fetch_alpha_vantage_daily(symbol, api_key)
    _, rows = _parse_alpha_vantage_daily_json(symbol, payload)
    upserted = upsert_rows(rows)
    result = {"symbol": symbol, "rows_upserted": upserted}
    print(f"Pipeline completed: {result}")
    return result


def _fetch_and_store(**context):
    symbol = os.getenv("STOCK_SYMBOL", "MSFT")
    result = run_pipeline(symbol)
    # Optional: push XCom for observability
    context["ti"].xcom_push(key="rows_upserted", value=result.get("rows_upserted"))


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="stock_pipeline_dag",
    default_args=default_args,
    description="Fetch daily stock data from Alpha Vantage and upsert to Postgres",
    schedule_interval=os.getenv("SCHEDULE_INTERVAL", "* * * * *"),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    fetch_and_store = PythonOperator(
        task_id="fetch_and_store",
        python_callable=_fetch_and_store,
        provide_context=True,
    )

    fetch_and_store


