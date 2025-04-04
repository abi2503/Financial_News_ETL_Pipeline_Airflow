from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

#Define function for each task using xcom

def start_number(**context):
    context["ti"].xcom_push(key="current_value",value=10)#The xcom acts as a temporary storage for furthur opertaions in the DAG
    print("Starting Number 10")


def add_five(**context):
    current_value=context['ti'].xcom_pull(key="current_value",task_ids="start_number")#task ID specifies the output representation of the previous task
    new_val=current_value+5
    context["ti"].xcom_push(key="current_value",value=new_val)

def multiply_by_two(**context):
    current_value=context["ti"].xcom_pull(key="current_value",task_ids="add_five")
    new_val=current_value*2
    context["ti"].xcom_push(key="current_value",value=new_val)


def subtract_two(**context):
    current_value=context["ti"].xcom_pull(key="current_value",task_ids="multiply_by_two")
    new_val=current_value-2
    context["ti"].xcom_push(key="current_value",value=new_val)


def square(**context):
    current_value=context["ti"].xcom_pull(key="current_value",task_ids="subtract_two")
    new_val=current_value**2
    context["ti"].xcom_push(key="current_value",value=new_val)


with DAG(
    "Mathematical_Operations",
    start_date=datetime(2025,3,24),
    schedule="@weekly",
    catchup=False

) as dag:
    start_number=PythonOperator(task_id="start_number",python_callable=start_number,provide_context=True)
    add_five=PythonOperator(task_id="add_five",python_callable=add_five,provide_context=True)
    multiply_by_two=PythonOperator(task_id="multiply_by_two",python_callable=multiply_by_two,provide_context=True)
    subtract_two=PythonOperator(task_id="subtract_two",python_callable=subtract_two,provide_context=True)
    square=PythonOperator(task_id="square_number",python_callable=square,provide_context=True)
    start_number>>add_five>>multiply_by_two>>subtract_two>>square
    
