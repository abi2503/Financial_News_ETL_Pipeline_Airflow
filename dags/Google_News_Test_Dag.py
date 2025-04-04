from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timezone, timedelta
import requests, feedparser, logging, re  # <-- Make sure `re` is here
from bs4 import BeautifulSoup
from newspaper import Article
from langchain.document_loaders import WebBaseLoader
from unidecode import unidecode


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

@dag(
    dag_id="test_google_news_parser_to_postgres",
    default_args=default_args,
    description="Test Google News article parsing and save to Postgres",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["debug", "google_news", "postgres"]
)
def google_news_parser_dag():

    def extract_full_text(url):
        logging.info(f"🧪 Extracting article from: {url}")

        try:
            article = Article(url)
            article.download()
            article.parse()
            text = article.text.strip()
            if text:
                logging.info("✅ Extracted using Newspaper3k")
                return unidecode(text)
        except Exception as e:
            logging.warning(f"❌ Newspaper3k failed: {e}")

        try:
            headers = {"User-Agent": "Mozilla/5.0"}
            response = requests.get(url, headers=headers, timeout=10)
            soup = BeautifulSoup(response.text, "lxml")
            paragraphs = soup.find_all("p")
            full_text = " ".join([p.get_text() for p in paragraphs if len(p.get_text().split()) > 5])
            if full_text.strip():
                logging.info("✅ Extracted using BeautifulSoup")
                return unidecode(full_text.strip())
        except Exception as e:
            logging.warning(f"❌ BeautifulSoup failed: {e}")

        try:
            loader = WebBaseLoader(url)
            docs = loader.load()
            if docs:
                logging.info("✅ Extracted using LangChain")
                return unidecode(docs[0].page_content.strip())
        except Exception as e:
            logging.warning(f"❌ LangChain loader failed: {e}")

        logging.error("🚫 All methods failed for article extraction")
        return "Full content not available"

    @task
    def fetch_google_news_articles():
        tickers = ["AAPL", "TSLA"]
        to_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        all_articles = []

        for ticker in tickers:
            url = f"https://news.google.com/rss/search?q={ticker}+stock+market&hl=en-US&gl=US&ceid=US:en"
            feed = feedparser.parse(url)
            logging.info(f"🌐 Google News returned {len(feed.entries)} entries for {ticker}")

            for entry in feed.entries:
                try:
                    # Try to extract original article URL from description
                    desc = entry.get("summary", "") or entry.get("description", "")
                    match = re.search(r'href=[\'"]?([^\'" >]+)', desc)

                    if match:
                        original_url = match.group(1)
                    else:
                        # Fall back to resolving the redirect manually
                        headers = {"User-Agent": "Mozilla/5.0"}
                        response = requests.get(entry.link, headers=headers, allow_redirects=True, timeout=10)
                        original_url = response.url

                    text = extract_full_text(original_url)

                    all_articles.append({
                        "article_id": original_url,
                        "ticker_symbol": ticker,
                        "article_title": entry.title,
                        "article_text": text,
                        "published_date": to_date,
                        "source": "Google News",
                        "author": "N/A"
                    })
                except Exception as e:
                    logging.error(f"❌ Failed to extract article for {ticker}: {e}")

        valid_articles = [a for a in all_articles if a["article_text"] != "Full content not available"]
        logging.info(f"✅ Total valid articles fetched: {len(valid_articles)}")
        return valid_articles

    @task
    def save_to_postgres(rows):
        hook = PostgresHook(postgres_conn_id="my_postgres_connection")
        insert_query = """
            INSERT INTO financial_news (
                article_id, ticker_symbol, article_title, article_text, published_date, source, author,
                reddit_mentions, word_count, sentence_count, readability_score,
                sentiment_vader, sentiment_finbert, named_entities
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, 0, 0, 0, 0, 0, 'N/A', 'N/A');
        """

        for row in rows:
            values = tuple(row.get(k) for k in [
                "article_id", "ticker_symbol", "article_title", "article_text", "published_date", "source", "author"
            ])
            hook.run(insert_query, parameters=values)

        logging.info("✅ All articles inserted into Postgres.")

    data = fetch_google_news_articles()
    save_to_postgres(data)

dag = google_news_parser_dag()
