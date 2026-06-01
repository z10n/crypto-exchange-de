import psycopg2

try:
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5433,  # <-- ВАЖНО: новый порт
        dbname="crypto_dw",
        user="admin",
        password="secret"
    )
    conn.set_client_encoding('UTF8')
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM gold_daily_metrics;")
    print("✅ Подключение успешно!")
    print(f"Строк в таблице: {cur.fetchone()[0]}")
    cur.close()
    conn.close()
except Exception as e:
    print(f"❌ Ошибка: {e}")