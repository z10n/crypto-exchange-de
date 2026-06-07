from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import sys
import os 
sys.path.append("/opt/airflow/dags")
from crypto_api_client import fetch_ohlcv

# 🚨 TELEGRAM ALERT CALLBACK (вставь ПОСЛЕ импортов)
def send_telegram_alert(context):
    import os
    import requests
    from datetime import datetime

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        print("⚠️ Telegram credentials not set. Skipping alert.")
        return

    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    exec_date = context.get('logical_date') or context.get('execution_date')
    exception = context.get('exception')
    error_msg = str(exception) if exception else "Unknown error"

    text = (
        f"🚨 *DAG FAILED*\n"
        f"📦 DAG: `{dag_id}`\n"
        f"🔧 Task: `{task_id}`\n"
        f"🕐 Date: `{exec_date.strftime('%Y-%m-%d %H:%M UTC')}`\n"
        f"❌ Error: `{error_msg[:200]}`"
    )

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}

    try:
        requests.post(url, json=payload, timeout=10)
        print("✅ Telegram alert sent.")
    except Exception as e:
        print(f"❌ Alert failed: {e}")

default_args = {
    "owner": "de_student",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2023, 1, 1),
    "on_failure_callback": send_telegram_alert,
}

dag = DAG(
    dag_id='etl_crypto_pipeline',
    default_args=default_args,
    schedule_interval='@daily',  
    catchup=False,               
    start_date=datetime(2024, 1, 1)
)

def extract_and_load_bronze(**kwargs):
    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
    all_data = []
    for sym in symbols:
        df = fetch_ohlcv(symbol=sym, interval="1d", limit=30)
        all_data.append(df)
    full_df = pd.concat(all_data, ignore_index=True)
    
    # 🔑 АВТОМАТИЧЕСКОЕ СОЗДАНИЕ ПАПОК
    output_path = "/opt/airflow/data/bronze/crypto_raw.parquet"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    full_df.to_parquet(output_path, index=False)
    print(f"✅ Extracted {len(full_df)} rows to {output_path}")

def load_to_silver(**kwargs):
    df = pd.read_parquet("/opt/airflow/data/bronze/crypto_raw.parquet")
    pg = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("TRUNCATE TABLE silver_ohlcv;")
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO silver_ohlcv (timestamp, symbol, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (timestamp, symbol) DO NOTHING;
        """, (row["open_time"], row["symbol"], row["open"], row["high"], row["low"], row["close"], row["volume"]))
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Loaded to Silver layer")

def transform_to_gold(**kwargs):
    pg = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO gold_daily_metrics (date, symbol, daily_return, volatility_7d, avg_volume)
        SELECT 
            DATE(timestamp),
            symbol,
            (close - LAG(close) OVER (PARTITION BY symbol ORDER BY timestamp)) / LAG(close) OVER (PARTITION BY symbol ORDER BY timestamp) as daily_return,
            STDDEV(close) OVER (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as volatility_7d,
            AVG(volume) OVER (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as avg_volume
        FROM silver_ohlcv
        ON CONFLICT (date, symbol) DO UPDATE SET
            daily_return = EXCLUDED.daily_return,
            volatility_7d = EXCLUDED.volatility_7d,
            avg_volume = EXCLUDED.avg_volume;
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Transformed to Gold layer")

# Задачи
extract = PythonOperator(task_id="extract_crypto", python_callable=extract_and_load_bronze, dag=dag)
load = PythonOperator(task_id="load_silver", python_callable=load_to_silver, dag=dag)
transform = PythonOperator(task_id="transform_gold", python_callable=transform_to_gold, dag=dag)

extract >> load >> transform