-- Создание таблиц для проекта Crypto Exchange
-- Этот файл выполнится автоматически при первом запуске контейнера PostgreSQL

-- Silver слой: очищенные OHLCV данные
CREATE TABLE IF NOT EXISTS silver_ohlcv (
    timestamp TIMESTAMPTZ NOT NULL,         -- Время открытия свечи (с часовым поясом)
    symbol VARCHAR(20) NOT NULL,            -- Тикер: BTCUSDT, ETHUSDT и т.д.
    open NUMERIC(18, 8),                    -- Цена открытия
    high NUMERIC(18, 8),                    -- Максимум
    low NUMERIC(18, 8),                     -- Минимум
    close NUMERIC(18, 8),                   -- Цена закрытия
    volume NUMERIC(20, 8),                  -- Объём торгов
    PRIMARY KEY (timestamp, symbol)         -- Составной ключ: время + тикер = уникальность
);

-- Индекс для ускорения запросов по символу
CREATE INDEX IF NOT EXISTS idx_silver_symbol ON silver_ohlcv(symbol);

-- Gold слой: агрегированные дневные метрики
CREATE TABLE IF NOT EXISTS gold_daily_metrics (
    date DATE NOT NULL,                     -- Дата
    symbol VARCHAR(20) NOT NULL,            -- Тикер
    daily_return NUMERIC(10, 6),            -- Дневная доходность (в долях: 0.05 = 5%)
    volatility_7d NUMERIC(18, 8),           -- Волатильность за 7 дней (stddev close)
    avg_volume NUMERIC(20, 8),              -- Средний объём за 7 дней
    PRIMARY KEY (date, symbol)              -- Составной ключ: дата + тикер
);

-- Индекс для ускорения запросов по дате
CREATE INDEX IF NOT EXISTS idx_gold_date ON gold_daily_metrics(date);