'''
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
from waybackpy import WaybackMachineCDXServerAPI
from bs4 import BeautifulSoup

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

    def get_full_article_text(url):
        try:
            article = Article(url)
            article.download()
            article.parse()
            text = article.text.strip()
            if text:
                return unidecode(text)
        except Exception:
            pass  

        try:
            headers = {"User-Agent": "Mozilla/5.0"}
            response = requests.get(url, headers=headers)
            soup = BeautifulSoup(response.text, "lxml")
            paragraphs = soup.find_all("p")
            full_text = " ".join([p.get_text() for p in paragraphs if len(p.get_text().split()) > 5])
            if full_text.strip():
                return unidecode(full_text.strip())
        except Exception:
            pass  

        try:
            wayback = WaybackMachineCDXServerAPI(url)
            archives = wayback.snapshots()
            if archives:
                archive_url = f"https://web.archive.org/web/{archives[0]['timestamp']}/{url}"
                response = requests.get(archive_url)
                soup = BeautifulSoup(response.text, "lxml")
                paragraphs = soup.find_all("p")
                full_text = " ".join([p.get_text() for p in paragraphs if len(p.get_text().split()) > 5])
                return unidecode(full_text.strip())
        except Exception:
            pass  

        return "Full content not available"

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

        def from_newsapi(ticker, NEWSAPI_KEY):
            url = (
                f"https://newsapi.org/v2/everything?"
                f"q={ticker}&from={from_date}&to={to_date}"
                f"&sortBy=publishedAt&pageSize=100&apiKey={NEWSAPI_KEY}"
            )
            logging.info(f"🔍 Fetching from NewsAPI for {ticker}: {url}")
            response = requests.get(url)
            try:
                res = response.json()
            except Exception as e:
                logging.error(f"❌ Error parsing NewsAPI JSON for {ticker}: {e}")
                return []

            if res.get("status") != "ok":
                logging.error(f"❌ NewsAPI error for {ticker}: {res.get('message')}")
                return []

            articles = res.get("articles", [])
            logging.info(f"✅ NewsAPI returned {len(articles)} articles for {ticker}")

            return [{
                "article_id": a.get("url"),
                "ticker_symbol": ticker,
                "article_title": a.get("title"),
                "article_text": get_full_article_text(a.get("url")),
                "published_date": to_date,
                "source": a["source"]["name"],
                "author": a.get("author", "N/A")
            } for a in articles]

        def from_google_news(ticker):
            url = f"https://news.google.com/rss/search?q={ticker}+stock+market"
            feed = feedparser.parse(url)
            logging.info(f"🌐 Google News returned {len(feed.entries)} entries for {ticker}")
            articles = []
            for i, entry in enumerate(feed.entries):
                text = get_full_article_text(entry.link)
                #logging.info(f"📰 Google News Article {i+1} Title: {entry.title}")
                #logging.info(f"📝 Extracted Text Snippet: {text[:300]}")
                articles.append({
                    "article_id": entry.link,
                    "ticker_symbol": ticker,
                    "article_title": entry.title,
                    "article_text": text,
                    "published_date": to_date,
                    "source": "Google News",
                    "author": "N/A"
                })
            return articles

        for ticker in tickers:
            all_articles.extend(from_newsapi(ticker, NEWSAPI_KEY))
            all_articles.extend(from_google_news(ticker))

        valid_articles = [a for a in all_articles if a["article_text"] != "Full content not available"]
        logging.info(f"📦 Total valid articles fetched: {len(valid_articles)}")
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
'''

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
from bs4 import BeautifulSoup
from langchain.document_loaders import WebBaseLoader

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

    def get_full_article_text(url):
        try:
            article = Article(url)
            article.download()
            article.parse()
            text = article.text.strip()
            if text:
                return unidecode(text)
        except Exception:
            pass

        try:
            headers = {"User-Agent": "Mozilla/5.0"}
            response = requests.get(url, headers=headers)
            soup = BeautifulSoup(response.text, "lxml")
            paragraphs = soup.find_all("p")
            full_text = " ".join([p.get_text() for p in paragraphs if len(p.get_text().split()) > 5])
            if full_text.strip():
                return unidecode(full_text.strip())
        except Exception:
            pass

        try:
            loader = WebBaseLoader(url)
            docs = loader.load()
            if docs:
                return unidecode(docs[0].page_content.strip())
        except Exception:
            pass

        return "Full content not available"

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

        def from_newsapi(ticker, NEWSAPI_KEY):
            url = (
                f"https://newsapi.org/v2/everything?"
                f"q={ticker}&from={from_date}&to={to_date}"
                f"&sortBy=publishedAt&pageSize=100&apiKey={NEWSAPI_KEY}"
            )
            logging.info(f"🔍 Fetching from NewsAPI for {ticker}: {url}")
            response = requests.get(url)
            try:
                res = response.json()
            except Exception as e:
                logging.error(f"❌ Error parsing NewsAPI JSON for {ticker}: {e}")
                return []

            if res.get("status") != "ok":
                logging.error(f"❌ NewsAPI error for {ticker}: {res.get('message')}")
                return []

            articles = res.get("articles", [])
            logging.info(f"✅ NewsAPI returned {len(articles)} articles for {ticker}")

            return [{
                "article_id": a.get("url"),
                "ticker_symbol": ticker,
                "article_title": a.get("title"),
                "article_text": get_full_article_text(a.get("url")),
                "published_date": to_date,
                "source": a["source"]["name"],
                "author": a.get("author", "N/A")
            } for a in articles]
        
        def from_google_news(ticker):
            url = f"https://news.google.com/rss/search?q={ticker}+stock+market"
            feed = feedparser.parse(url)
            logging.info(f"🌐 Google News returned {len(feed.entries)} entries for {ticker}")
            articles = []
            for entry in feed.entries:
                text = get_full_article_text(entry.link)
                articles.append({
                    "article_id": entry.link,
                    "ticker_symbol": ticker,
                    "article_title": entry.title,
                    "article_text": text,
                    "published_date": to_date,
                    "source": "Google News",
                    "author": "N/A"
                })
            return articles
        '''
        def from_google_news(ticker):
            url = f"https://news.google.com/rss/search?q={ticker}+stock+market"
            feed = feedparser.parse(url)
            logging.info(f"🌐 Google News returned {len(feed.entries)} entries for {ticker}")
            articles = []

            for entry in feed.entries:
                try:
                    # Follow redirect to get the final article link
                    headers = {"User-Agent": "Mozilla/5.0"}
                    response = requests.get(entry.link, headers=headers, allow_redirects=True, timeout=10)
                    final_url = response.url

                    text = get_full_article_text(final_url)
                    articles.append({
                        "article_id": final_url,
                        "ticker_symbol": ticker,
                        "article_title": entry.title,
                        "article_text": text,
                        "published_date": to_date,
                        "source": "Google News",
                        "author": "N/A"
                    })
                except Exception as e:
                    logging.error(f"❌ Error following redirect for Google News link: {entry.link} -> {e}")

            return articles
        '''

        for ticker in tickers:
            all_articles.extend(from_newsapi(ticker, NEWSAPI_KEY))
            all_articles.extend(from_google_news(ticker))

        valid_articles = [a for a in all_articles if a["article_text"] != "Full content not available"]
        logging.info(f"📦 Total valid articles fetched: {len(valid_articles)}")
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
