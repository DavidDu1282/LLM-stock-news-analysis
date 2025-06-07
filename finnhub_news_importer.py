import logging
from datetime import datetime
from pymongo import MongoClient, UpdateOne
from market_data.finnhub_adapter import FinnhubAdapter
import asyncio

# MongoDB config
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "Finnhub_News"
COLLECTION_NAME = "market_news"

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def fetch_and_save_finnhub_news():
    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    # Fetch news from Finnhub
    adapter = FinnhubAdapter()
    news_items = await adapter.get_latest_market_news(category='general')
    if not news_items:
        logger.warning("No news items fetched from Finnhub.")
        return

    logger.info(f"Fetched {len(news_items)} news items from Finnhub.")
    operations = []
    new_count = 0
    for item in news_items:
        url = item.get('url')
        news_id = item.get('id')
        if not url and not news_id:
            continue
        # Deduplicate by url or id
        query = {"$or": [{"url": url}]}
        if news_id is not None:
            query["$or"].append({"finnhub_id": news_id})
        existing = collection.find_one(query)
        if existing:
            continue
        # Prepare document using base_crawler schema
        doc = {
            "url": url,
            "title": item.get('headline', 'N/A'),
            "source_page_url": "finnhub_api_general",  # API endpoint/category
            "content": item.get('summary', ''),
            "source": "finnhub_api",
            "source_db": DB_NAME,
            "source_collection": COLLECTION_NAME,
            "fetched_at": datetime.now(),
            "published_date": datetime.fromtimestamp(item.get('datetime')) if item.get('datetime') else None,
            "analyzed": False,
            # Extra fields for traceability
            "finnhub_id": news_id,
            "category": item.get('category'),
            "image": item.get('image'),
            "news_source": item.get('source'),
        }
        operations.append(UpdateOne({"url": url}, {"$set": doc}, upsert=True))
        new_count += 1
    if operations:
        result = collection.bulk_write(operations)
        logger.info(f"Inserted/Upserted {result.upserted_count} new Finnhub news articles.")
    else:
        logger.info("No new Finnhub news articles to insert.")
    logger.info(f"Total new articles processed: {new_count}")

if __name__ == "__main__":
    asyncio.run(fetch_and_save_finnhub_news()) 