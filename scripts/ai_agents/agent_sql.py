# scripts/ai_agents/agent_sql.py
import os
import sys
import warnings

# Подавляем предупреждения о deprecated пакетах для чистого вывода
warnings.filterwarnings("ignore", category=DeprecationWarning)

# Добавляем корень проекта в PYTHONPATH, чтобы импорты работали из любой директории
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from langchain_ollama import ChatOllama
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import create_sql_agent
from scripts.ai_agents.tools.airflow_tool import check_dag_status

def main():
    print("🤖 Инициализация AI-агента...")

    # 1. Подключаем локальную модель Ollama
    llm = ChatOllama(
        model="llama3.2",
        temperature=0,      # 0 = точные ответы, без "творчества"
        verbose=False
    )
    print("✅ Модель Llama 3.2 подключена")

    # 2. Подключение к PostgreSQL с ограничениями безопасности
    DATABASE_URL = "postgresql+psycopg2://admin:secret@127.0.0.1:5433/crypto_dw"
    db = SQLDatabase.from_uri(
        DATABASE_URL,
        include_tables=["gold_daily_metrics", "silver_ohlcv"], # 🔒 Агент видит только эти таблицы
        sample_rows_in_table_info=3                            # 💡 Даём 3 примера строк для контекста
    )
    print("✅ Подключение к PostgreSQL установлено")

    # 3. Создание агента с кастомными инструментами
    agent = create_sql_agent(
        llm=llm,
        db=db,
        agent_type="openai-tools",
        verbose=True,          # Показывает "ход мыслей" агента в консоли
        handle_parsing_errors=True,
        extra_tools=[check_dag_status] # 🔧 Добавляем твой инструмент проверки Airflow
    )
    print("✅ Агент создан и готов к работе\n")

    # 4. Интерактивный режим
    print("="*60)
    print("🚀 AI-агент запущен! Задавай вопросы (или 'выход' для завершения)")
    print("="*60)

    while True:
        try:
            user_input = input("\n🔍 Ты: ").strip()

            if user_input.lower() in ["выход", "exit", "quit", "q"]:
                print("👋 Пока! Агент остановлен.")
                break

            if not user_input:
                continue

            print("\n⏳ Агент думает...")
            response = agent.invoke({"input": user_input})

            print(f"\n✅ Агент: {response['output']}")
            print("-" * 60)

        except KeyboardInterrupt:
            print("\n\n👋 Прервано пользователем. Пока!")
            break
        except Exception as e:
            print(f"\n❌ Ошибка: {e}")
            print("💡 Попробуй перефразировать вопрос или перезапусти агента.")

if __name__ == "__main__":
    main()