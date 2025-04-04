from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timezone, timedelta
import requests, feedparser, logging
import nltk, spacy, textstat, praw
from newspaper import Article
from transformers import pipeline
from unidecode import unidecode

nltk.download("punkt")

# 🔐 Secrets
NEWSAPI_KEY = Variable.get("newsapi_key")
REDDIT_CLIENT_ID = Variable.get("reddit_client_id")
REDDIT_CLIENT_SECRET = Variable.get("reddit_client_secret")
REDDIT_USER_AGENT = Variable.get("reddit_user_agent")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="step5_load_postgres_dag",
    default_args=default_args,
    description="Step 5: Enrich articles and load to PostgreSQL",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=["step-by-step", "postgres"]
)
def step5_dag():

    @task
    def create_table():
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
                author VARCHAR(255),
                reddit_mentions INT,
                word_count INT,
                sentence_count INT,
                readability_score FLOAT,
                sentiment_vader FLOAT,
                sentiment_finbert VARCHAR(50),
                named_entities TEXT
            );
        """)

    @task
    def fetch_articles():
        tickers = ["AAPL", "TSLA", "GOOGL"]
        to_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        from_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        all_articles = []

        def extract_text(url):
            try:
                article = Article(url)
                article.download()
                article.parse()
                return unidecode(article.text.strip())
            except Exception:
                return "Full content not available"

        def from_newsapi(ticker, NEWSAPI_KEY):
            url = (
                f"https://newsapi.org/v2/everything?"
                f"q={ticker}&from={from_date}&to={to_date}"
                f"&sortBy=publishedAt&pageSize=100&apiKey={NEWSAPI_KEY}"
            )
            logging.info(f"🔍 Fetching from NewsAPI for {ticker}: {url}")
            response = requests.get(url)
            try:
                logging.info(f"🔐 NEWSAPI_KEY passed in Airflow: {NEWSAPI_KEY}")
                res = response.json()
                

            except Exception as e:
                logging.error(f"❌ Error parsing NewsAPI JSON for {ticker}: {e}")
                return []

            if res.get("status") != "ok":
                logging.error(f"❌ NewsAPI error for {ticker}: {res.get('message')}")
                logging.info(res.get("status"))
                return []

            articles = res.get("articles", [])
            logging.info(f"✅ NewsAPI returned {len(articles)} articles for {ticker}")

            return [{
                "article_id": a.get("url"),
                "ticker_symbol": ticker,
                "article_title": a.get("title"),
                "article_text": extract_text(a.get("url")),
                "published_date": to_date,
                "source": a["source"]["name"],
                "author": a.get("author", "N/A")
            } for a in articles]

        def from_google_news(ticker):
            url = f"https://news.google.com/rss/search?q={ticker}+stock+market"
            feed = feedparser.parse(url)
            logging.info(f"🌐 Google News returned {len(feed.entries)} entries for {ticker}")
            return [{
                "article_id": entry.link,
                "ticker_symbol": ticker,
                "article_title": entry.title,
                "article_text": extract_text(entry.link),
                "published_date": to_date,
                "source": "Google News",
                "author": "N/A"
            } for entry in feed.entries]

        for ticker in tickers:
            newsapi_articles = from_newsapi(ticker, NEWSAPI_KEY)
            googlenews_articles = from_google_news(ticker)
            all_articles.extend(newsapi_articles)
            all_articles.extend(googlenews_articles)

        valid_articles = [a for a in all_articles if a["article_text"] != "Full content not available"]
        logging.info(f"📦 Total valid articles fetched: {len(valid_articles)}")
        if valid_articles:
            logging.info(f"📰 Sample article: {valid_articles[0]['article_title']} from {valid_articles[0]['source']}")
        return valid_articles

    @task
    def enrich_articles(articles):
        nlp = spacy.load("en_core_web_sm")
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        vader = SentimentIntensityAnalyzer()
        finbert = pipeline("sentiment-analysis", model="yiyanghkust/finbert-tone")
        reddit = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT
        )

        enriched = []

        def get_reddit_mentions(ticker):
            try:
                return sum(1 for _ in reddit.subreddit("wallstreetbets").search(ticker, limit=50))
            except Exception:
                return 0

        for a in articles:
            text = a["article_text"]

            enriched.append({
                **a,
                "reddit_mentions": get_reddit_mentions(a["ticker_symbol"]),
                "word_count": len(text.split()),
                "sentence_count": len(nltk.sent_tokenize(text)),
                "readability_score": textstat.flesch_reading_ease(text),
                "sentiment_vader": vader.polarity_scores(text)["compound"],
                "sentiment_finbert": finbert(text[:512])[0]["label"],
                "named_entities": ", ".join([ent.text for ent in nlp(text).ents if ent.label_ in ["ORG", "PERSON"]])
            })

        return enriched

    @task
    def load_to_postgres(rows):
        hook = PostgresHook(postgres_conn_id="my_postgres_connection")
        insert_query = """
            INSERT INTO financial_news (
                article_id, ticker_symbol, article_title, article_text, published_date, source, author,
                reddit_mentions, word_count, sentence_count, readability_score,
                sentiment_vader, sentiment_finbert, named_entities
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        for row in rows:
            values = tuple(row.get(k) for k in [
                "article_id", "ticker_symbol", "article_title", "article_text", "published_date", "source", "author",
                "reddit_mentions", "word_count", "sentence_count", "readability_score",
                "sentiment_vader", "sentiment_finbert", "named_entities"
            ])
            hook.run(insert_query, parameters=values)

    create_table()
    raw = fetch_articles()
    enriched = enrich_articles(raw)
    load_to_postgres(enriched)

dag = step5_dag()
