from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import timedelta

# ✅ DAG defaults
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="step1_basic_dag",
    default_args=default_args,
    description="Step 1: Hello DAG",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=["step-by-step"]
)
def hello_decorator_dag():

    @task
    def print_hello():
        print("👋 Hello from Step 1 DAG!")

    print_hello()

# ✅ Register the DAG with Airflow
dag = hello_decorator_dag()
