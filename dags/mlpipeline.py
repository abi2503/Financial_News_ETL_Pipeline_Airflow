from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

##Define a task

def preprocess_data():
    print("Preprocessing_Data")

def train_model():
    print("Trining models")


def evaluate_model():
    print("Evaluate Models")

#Define DAG

with DAG(
    "ML_Pipeline",
    start_date=datetime(2025,3,24),
    schedule='@weekly',
) as dag:
    #define task
    preprocess=PythonOperator(task_id='Preprocessing_Data',python_callable=preprocess_data)#Python operator is defining the task
    train=PythonOperator(task_id="Train_Model",python_callable=train_model)
    evaluate=PythonOperator(task_id="Evaluate_Model",python_callable=evaluate_model)

    ##set dependencies
    preprocess>>train>>evaluate