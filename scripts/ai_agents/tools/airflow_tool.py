# scripts/ai_agents/tools/airflow_tool.py
import requests
from requests.auth import HTTPBasicAuth
from langchain_core.tools import tool

@tool
def check_dag_status(dag_id: str) -> str:
    """
    Проверяет статус ПОСЛЕДНЕГО запуска DAG в Airflow через REST API.
    
    🔹 Используй этот инструмент, когда пользователь спрашивает:
    - "Проверь статус пайплайна / DAG"
    - "Когда последний раз запускался..."
    - "Успешно ли отработал..."
    - "Какой статус у crypto_etl_pipeline"
    
    🔹 НЕ используй для вопросов про данные в таблицах БД.
    
    Args:
        dag_id: ID дага, например 'crypto_etl_pipeline'
    
    Returns:
        Строка с результатом: статус, время запуска, количество успешных задач
    """
    AIRFLOW_URL = "http://localhost:8080/api/v1"
    AUTH = HTTPBasicAuth("admin", "admin")
    
    try:
        # 1. Получаем информацию о последнем запуске
        response = requests.get(
            f"{AIRFLOW_URL}/dags/{dag_id}/dagRuns?limit=1&order_by=-start_date",
            auth=AUTH,
            timeout=10
        )
        response.raise_for_status()
        data = response.json()
        
        if not data.get("dag_runs"):
            return f"❌ Не найдено запусков для DAG '{dag_id}'"
        
        last_run = data["dag_runs"][0]
        status = last_run["state"]
        start_date = last_run["start_date"]
        run_id = last_run["dag_run_id"]
        
        # 2. Получаем статистику задач
        tasks_resp = requests.get(
            f"{AIRFLOW_URL}/dags/{dag_id}/dagRuns/{run_id}/taskInstances",
            auth=AUTH,
            timeout=10
        )
        tasks_resp.raise_for_status()
        tasks_data = tasks_resp.json()
        
        total = len(tasks_data["task_instances"])
        success = sum(1 for t in tasks_data["task_instances"] if t["state"] == "success")
        
        return (f"✅ DAG: {dag_id}\n"
                f"🕐 Запущен: {start_date}\n"
                f"📊 Статус: {status}\n"
                f"🔧 Задач: {success}/{total} успешно")
                
    except requests.exceptions.ConnectionError:
        return "❌ Не удалось подключиться к Airflow. Проверь, запущен ли контейнер."
    except requests.exceptions.HTTPError as e:
        return f"❌ Ошибка API: {e.response.status_code} — {e.response.text}"
    except Exception as e:
        return f"❌ Неожиданная ошибка: {str(e)}"