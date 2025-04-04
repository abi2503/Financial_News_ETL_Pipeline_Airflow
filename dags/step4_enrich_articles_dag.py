from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests, feedparser, logging, time
import nltk, spacy, textstat, praw
from newspaper import Article
from transformers import pipeline
from unidecode import unidecode
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# ✅ Load NLP models inside the task
nltk.download("punkt")

# ✅ Config
NEWSAPI_KEY = Variable.get("newsapi_key", default_var=None)
REDDIT_CLIENT_ID = Variable.get("reddit_client_id")
REDDIT_CLIENT_SECRET = Variable.get("reddit_client_secret")
REDDIT_USER_AGENT = Variable.get("reddit_user_agent")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="step4_enrich_articles_dag",
    default_args=default_args,
    description="Step 4: Enrich articles with NLP features",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=["step-by-step", "nlp", "enrich"]
)
def step4_dag():

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
        return valid_articles

    @task
    def enrich_articles(articles):
        # ✅ Load NLP models inside task
        nlp = spacy.load("en_core_web_sm")
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

        logging.info(f"🧠 Enriched {len(enriched)} articles.")
        logging.info(f"🧾 Sample: {enriched[0]['article_title']} | Sentiment: {enriched[0]['sentiment_finbert']}")
        return enriched

    raw_articles = fetch_articles()
    create_table() >> raw_articles >> enrich_articles(raw_articles)


dag = step4_dag()
