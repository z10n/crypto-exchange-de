import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

st.set_page_config(page_title="📈 Crypto Exchange Analytics", layout="wide")
st.title("📈 Crypto Market Data Pipeline Dashboard")

@st.cache_data(ttl=3600)
def load_data():
    try:
        # 🔑 Используем порт 5433 и драйвер pg8000
        engine = create_engine("postgresql+pg8000://admin:secret@127.0.0.1:5433/crypto_dw")
        df = pd.read_sql("SELECT * FROM gold_daily_metrics ORDER BY date DESC", engine)
        return df
    except Exception as e:
        st.error(f"Ошибка подключения к БД: {e}")
        return pd.DataFrame()

df = load_data()

if df.empty:
    st.warning("Нет данных в таблице gold_daily_metrics. Запустите DAG в Airflow.")
    st.stop()

# KPI метрики
c1, c2, c3 = st.columns(3)
c1.metric("Активов", df["symbol"].nunique())
c2.metric("Ср. волатильность (7д)", f"{df['volatility_7d'].mean():.4f}")
c3.metric("Последняя дата", df["date"].max().strftime("%Y-%m-%d"))

# Выбор актива
selected = st.selectbox("Выбери актив", sorted(df["symbol"].unique()))
sub = df[df["symbol"] == selected].sort_values("date")

# График доходности
fig = px.line(sub, x="date", y="daily_return", title=f"📈 Дневная доходность {selected}", markers=True)
st.plotly_chart(fig, use_container_width=True)

# Волатильность
fig2 = px.bar(sub, x="date", y="volatility_7d", title=f"📊 Волатильность {selected} (7д)")
st.plotly_chart(fig2, use_container_width=True)