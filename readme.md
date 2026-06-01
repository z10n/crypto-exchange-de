# 📈 Crypto Exchange Data Pipeline

End-to-end ETL пайплайн для сбора, обработки и визуализации рыночных данных криптовалют.
Проект демонстрирует навыки построения надежных данных конвейеров, работы с таймсериями и автоматизации процессов.

## 🏗️ Архитектура

A[Binance API] -->|Python Requests| B(Bronze: Parquet)
B -->|Airflow DAG| C[Silver: PostgreSQL]
C -->|SQL Window Functions| D[Gold: Metrics]
D -->|SQLAlchemy| E[Streamlit Dashboard]

**🛠️ Технологический стек**
Orchestration: Apache Airflow 2.7 (Docker)
Storage: PostgreSQL 15
Processing: Python (Pandas), SQL (Window Functions, CTE)
Visualization: Streamlit, Plotly
Infrastructure: Docker, Docker Compose
Key Concepts: ETL, Idempotency, Data Quality, Star Schema, Time Series Analysis

**🚀 Быстрый старт**
**1. Запуск инфраструктуры**

docker compose up -d

⏳ Подождите ~60 секунд, пока Airflow полностью запустится.

**##2. Настройка Airflow**
Откройте http://localhost:8080 (логин/пароль: admin / admin).
Перейдите в Admin → Connections → +.
Создайте новое подключение:
Connection Id: postgres_default
Connection Type: Postgres
Host: postgres-project
Schema: crypto_dw
Login: admin
Password: secret
Port: 5432

**##3. Запуск пайплайна**
Включите тумблер у DAG crypto_etl_pipeline.
Нажмите ▶️ Trigger DAG.
Дождитесь, пока все задачи станут зелеными ✅.

**##4. Запуск дашборда**
⚠️ Важно: Мы используем порт 5433, чтобы избежать конфликтов с локальным PostgreSQL.

Откройте http://localhost:8501.
📊 Метрики и трансформации
Daily Return: (close - prev_close) / prev_close
Volatility (7d): STDDEV(close) OVER (ROWS 6 PRECEDING)
Avg Volume (7d): Скользящее среднее объема торгов

## ✅ Data Quality Checks
Пайплайн включает автоматические проверки:
Отсутствие NULL значений в ключевых метриках.
Проверка на отрицательную волатильность.
Валидация цен (close > 0).
🔮 Future Improvements
Интеграция с Kafka для real-time данных.
Использование dbt для управления трансформациями.
Деплой дашборда в облако (Streamlit Cloud).
Добавление тестов данных через Great Expectations.

## 📷 Результаты

![Airflow DAG](assets/dag_graph.png)
![Dashboard](assets/dashboard.png)
![Data](assets/db_data.png)
