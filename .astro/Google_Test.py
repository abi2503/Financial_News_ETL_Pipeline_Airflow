import feedparser
from newspaper import Article
import logging

logging.basicConfig(level=logging.INFO)

# Step 1: Parse RSS and extract real publisher URL from source.href
def get_real_article_url_from_rss(rss_feed_url):
    feed = feedparser.parse(rss_feed_url)
    if not feed.entries:
        logging.warning("❌ No entries found in RSS feed.")
        return None

    for entry in feed.entries:
        title = entry.title
        google_link = entry.link
        real_link = entry.get("source", {}).get("href", None)

        logging.info(f"🔗 Title: {title}")
        logging.info(f"📎 Google Redirect: {google_link}")
        logging.info(f"✅ Real Publisher Link: {real_link}")

        return real_link or google_link

# Step 2: Extract full article text
def get_full_article_text(url):
    try:
        article = Article(url)
        article.download()
        article.parse()
        return article.text
    except Exception as e:
        logging.warning(f"⚠️ Failed to parse article from {url}: {e}")
        return ""

# Main test
if __name__ == "__main__":
    rss_url = "https://news.google.com/rss/search?q=AAPL+stock+market&hl=en-US&gl=US&ceid=US:en"

    article_url = get_real_article_url_from_rss(rss_url)
    if article_url:
        article_text = get_full_article_text(article_url)
        if article_text:
            print("✅ Article Text:\n")
            print(article_text[:3000])
        else:
            print("❌ Failed to extract full article content.")
    else:
        print("❌ Could not retrieve article from RSS feed.")
