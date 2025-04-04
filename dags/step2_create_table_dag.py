from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import timedelta
import logging

# ✅ Default args
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="step2_create_table_dag",
    default_args=default_args,
    description="Step 2: Create table in Postgres",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=["step-by-step"]
)
def step2_dag():

    @task
    def create_table():
        logging.info("🧱 Connecting to Postgres...")

        # Replace with your Postgres connection ID from Airflow
        pg_hook = PostgresHook(postgres_conn_id="my_postgres_connection")

        create_sql = """
        CREATE TABLE IF NOT EXISTS financial_news (
            id SERIAL PRIMARY KEY,
            article_id TEXT,
            ticker_symbol VARCHAR(10),
            article_title TEXT,
            article_text TEXT,
            published_date DATE,
            source VARCHAR(255),
            author VARCHAR(255),
            reddit_mentions INT,
            word_count INT,
            sentence_count INT,
            readability_score FLOAT,
            sentiment_vader FLOAT,
            sentiment_finbert VARCHAR(50),
            named_entities TEXT,
            key_phrases TEXT
        );
        """

        pg_hook.run(create_sql)
        logging.info("✅ Table created or already exists.")

    create_table()

dag = step2_dag()
