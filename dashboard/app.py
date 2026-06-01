import streamlit as st
import pandas as pd
import plotly.express as px
from airflow.providers.postgres.hooks.postgres import PostgresHook

st.set_page_config(page_title="📈 Crypto Exchange Analytics", layout="wide")
st.title("📈 Market Data Pipeline Dashboard")

@st.cache_data(ttl=3600)
def load_data():
    pg = PostgresHook(postgres_conn_id="postgres_default")
    return pg.get_pandas_df("SELECT * FROM gold_daily_metrics ORDER BY date DESC")

df = load_data()
if df.empty:
    st.warning("Нет данных. Запусти DAG.")
    st.stop()

c1, c2, c3 = st.columns(3)
c1.metric("Активов в анализе", df["symbol"].nunique())
c2.metric("Средняя волатильность", f"{df['volatility_7d'].mean():.2f}")
c3.metric("Последняя дата", df["date"].max().strftime("%Y-%m-%d"))

selected = st.selectbox("Выбери актив", df["symbol"].unique())
sub = df[df["symbol"] == selected].sort_values("date")

fig = px.line(sub, x="date", y="close", title=f"📈 Цена {selected}", markers=True)
st.plotly_chart(fig, use_container_width=True)

fig2 = px.bar(sub, x="date", y="volume", title="📦 Объем торгов")
st.plotly_chart(fig2, use_container_width=True)