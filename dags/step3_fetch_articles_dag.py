from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import timedelta, datetime
import requests
import feedparser
from newspaper import Article
from unidecode import unidecode
import logging

# ✅ Logging setup
logging.basicConfig(level=logging.INFO)

# ✅ Airflow Variables
NEWSAPI_KEY = Variable.get("newsapi_key", default_var=None)

# ✅ Default args
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="step3_fetch_articles_dag",
    default_args=default_args,
    description="Step 3: Fetch financial news articles",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=["step-by-step", "news", "fetch"]
)
def step3_dag():

    @task
    def create_table():
        logging.info("🧱 Creating table if not exists...")
        hook = PostgresHook(postgres_conn_id="my_postgres_connection")
        hook.run("""
            CREATE TABLE IF NOT EXISTS financial_news (
                id SERIAL PRIMARY KEY,
                article_id TEXT,
                ticker_symbol VARCHAR(10),
                article_title TEXT,
                article_text TEXT,
                published_date DATE,
                source VARCHAR(255),
                author VARCHAR(255)
            );
        """)
        logging.info("✅ Table is ready.")

    @task
    def fetch_articles():
        tickers = ["AAPL", "TSLA", "GOOGL"]
        today = datetime.utcnow().strftime("%Y-%m-%d")
        all_articles = []

        def extract_text(url):
            try:
                article = Article(url)
                article.download()
                article.parse()
                return unidecode(article.text.strip())
            except Exception:
                return "Full content not available"

        def from_newsapi(ticker):
            url = f"https://newsapi.org/v2/everything?q={ticker}&from={today}&to={today}&sortBy=publishedAt&pageSize=100&apiKey={NEWSAPI_KEY}"
            res = requests.get(url).json()
            return [{
                "article_id": a.get("url"),
                "ticker_symbol": ticker,
                "article_title": a.get("title"),
                "article_text": extract_text(a.get("url")),
                "published_date": today,
                "source": a["source"]["name"],
                "author": a.get("author", "N/A")
            } for a in res.get("articles", [])]

        def from_google_news(ticker):
            url = f"https://news.google.com/rss/search?q={ticker}+stock+market"
            feed = feedparser.parse(url)
            return [{
                "article_id": entry.link,
                "ticker_symbol": ticker,
                "article_title": entry.title,
                "article_text": extract_text(entry.link),
                "published_date": today,
                "source": "Google News",
                "author": "N/A"
            } for entry in feed.entries]

        for ticker in tickers:
            all_articles.extend(from_newsapi(ticker))
            all_articles.extend(from_google_news(ticker))

        valid_articles = [a for a in all_articles if a["article_text"] != "Full content not available"]
        logging.info(f"✅ Collected {len(valid_articles)} valid articles")


        # 🔍 Print first 1–2 article titles to confirm content
        if valid_articles:
            logging.info(f"📰 First article title: {valid_articles[0]['article_title']}")
            logging.info(f"🔗 First article URL: {valid_articles[0]['article_id']}")

        return valid_articles

    create_table() >> fetch_articles()

dag = step3_dag()
