from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import requests
import spacy
import nltk
import textstat
import feedparser
import time
import praw
import yake
from bs4 import BeautifulSoup
from newspaper import Article
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from transformers import pipeline
from unidecode import unidecode
import logging

# ✅ Initialize NLP Models
nltk.download("punkt")
vader_analyzer = SentimentIntensityAnalyzer()
finbert = pipeline("sentiment-analysis", model="yiyanghkust/finbert-tone")
nlp = spacy.load("en_core_web_sm")

# ✅ API KEYS
API_KEYS = {
    "newsapi": "1b25aee04d374782b30f11cebf11a484",
    "reddit_client_id": "7hrh1y-tiTPJ7dWM5LBBmA",
    "reddit_client_secret": "S2NityFUj65coY6reHPQNIaViJgLrA",
    "reddit_user_agent": "testscript by u/stealthpotato02"
}

# ✅ Initialize Reddit API
reddit = praw.Reddit(
    client_id=API_KEYS["reddit_client_id"],
    client_secret=API_KEYS["reddit_client_secret"],
    user_agent=API_KEYS["reddit_user_agent"]
)

# ✅ Get Dates
today_date = datetime.utcnow().strftime("%Y-%m-%d")
yesterday_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

# ✅ Define Table Creation Function
def create_table():
    postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
    create_table_query = """
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
    postgres_hook.run(create_table_query)

# ✅ Define News Extraction Functions
def extract_named_entities(text):
    if not isinstance(text, str) or text.strip() == "":
        return []
    doc = nlp(text)
    return [ent.text for ent in doc.ents if ent.label_ in ["ORG", "PERSON"]]

def extract_key_phrases(text):
    kw_extractor = yake.KeywordExtractor(n=2, dedupLim=0.9, top=5, features=None)
    keywords = kw_extractor.extract_keywords(text)
    return [kw[0] for kw in keywords]

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

    return "Full content not available"

def fetch_newsapi_news(ticker):
    url = f"https://newsapi.org/v2/everything?q={ticker}&from={today_date}&to={today_date}&sortBy=publishedAt&pageSize=100&apiKey={API_KEYS['newsapi']}"
    response = requests.get(url)
    data = response.json()
    
    if "articles" in data and len(data["articles"]) > 0:
        return [{
            "article_id": article.get("url", "N/A"),
            "ticker_symbol": ticker,
            "article_title": article["title"],
            "article_text": get_full_article_text(article.get("url", "N/A")),
            "published_date": today_date,
            "source": article["source"]["name"],
            "author": article.get("author", "N/A")
        } for article in data["articles"]]

    return []

def fetch_google_news(ticker):
    url = f"https://news.google.com/rss/search?q={ticker}+stock+market"
    feed = feedparser.parse(url)

    articles = []
    for entry in feed.entries:
        articles.append({
            "article_id": entry.link,
            "ticker_symbol": ticker,
            "article_title": entry.title,
            "article_text": get_full_article_text(entry.link),
            "published_date": today_date,
            "source": "Google News"
        })
    return articles

def fetch_reddit_mentions(ticker):
    return sum(1 for _ in reddit.subreddit("wallstreetbets").search(ticker, limit=100))

# ✅ Extract & Process Data
def fetch_and_process_data():
    logging.info("Fetching financial news data...")
    
    tickers = ["AAPL", "TSLA", "GOOGL"]
    news_data = []

    for ticker in tickers:
        news_data.extend(fetch_newsapi_news(ticker))  
        news_data.extend(fetch_google_news(ticker))  
        time.sleep(1)

    news_data = [article for article in news_data if article["article_text"] != "Full content not available"]
    
    news_df = pd.DataFrame(news_data)
    news_df["reddit_mentions"] = news_df["ticker_symbol"].apply(fetch_reddit_mentions)
    news_df["word_count"] = news_df["article_text"].apply(lambda x: len(x.split()))
    news_df["sentence_count"] = news_df["article_text"].apply(lambda x: len(nltk.sent_tokenize(x)))
    news_df["readability_score"] = news_df["article_text"].apply(lambda x: textstat.flesch_reading_ease(x))
    news_df["sentiment_vader"] = news_df["article_text"].apply(lambda x: vader_analyzer.polarity_scores(x)["compound"] if isinstance(x, str) else 0)
    news_df["sentiment_finbert"] = news_df["article_text"].apply(lambda x: finbert(x[:512])[0]["label"] if isinstance(x, str) else "neutral")
    news_df["named_entities"] = news_df["article_text"].apply(extract_named_entities)
    news_df["key_phrases"] = news_df["article_text"].apply(extract_key_phrases)

    load_data_to_postgres(news_df)

# ✅ Load Data to PostgreSQL
def load_data_to_postgres(df):
    postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
    insert_query = """
        INSERT INTO financial_news (article_id, ticker_symbol, article_title, article_text, published_date, source, author, reddit_mentions, word_count, sentence_count, readability_score, sentiment_vader, sentiment_finbert, named_entities, key_phrases)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    for _, row in df.iterrows():
        postgres_hook.run(insert_query, parameters=tuple(row))

# ✅ Define DAG
default_args = {'owner': 'airflow', 'start_date': datetime(2024, 3, 13), 'retries': 1, 'retry_delay': timedelta(minutes=5)}

dag = DAG('fetch_financial_news_dag', default_args=default_args, description='Fetch financial news & store in PostgreSQL', schedule_interval='@daily', catchup=False)

create_table_task = PythonOperator(task_id='create_table', python_callable=create_table, dag=dag)
fetch_data_task = PythonOperator(task_id='fetch_financial_news', python_callable=fetch_and_process_data, dag=dag)

create_table_task >> fetch_data_task
