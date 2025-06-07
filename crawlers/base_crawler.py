import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Dict, Optional

import httpx  # Modern asynchronous HTTP client
from bs4 import BeautifulSoup # For parsing HTML
from pymongo import MongoClient, UpdateOne
from pymongo.errors import ConnectionFailure, OperationFailure

# Configure logging
logger = logging.getLogger(__name__)

class BaseCrawler(ABC):
    """
    Abstract base class for news crawlers.
    Requires subclasses to implement methods for fetching and parsing articles.
    """

    def __init__(self, db_name: str, collection_name: str, mongo_uri: str = "mongodb://localhost:27017/"):
        self.db_name = db_name
        self.collection_name = collection_name
        self.mongo_uri = mongo_uri
        self.client: Optional[MongoClient] = None
        self.db = None
        self.collection = None
        self._connect_db()
        self.retry_delay: int = 5 # Default retry delay in seconds
        self.force_encoding: Optional[str] = None # Allow crawlers to force an encoding

        # Use a shared httpx.AsyncClient for connection pooling and performance
        # Configure with appropriate timeouts and headers
        self.http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, read=40.0, write=10.0, connect=10.0), # seconds; increased overall and read timeouts
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            },
            # Enable HTTP/2 if supported by the server, can improve performance
            http2=True,
            # Follow redirects by default, common for news sites
            follow_redirects=True
        )
        logger.info(f"{self.__class__.__name__} initialized for {db_name}/{collection_name}")

    def _connect_db(self):
        """Establishes connection to MongoDB."""
        try:
            self.client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000) # 5 second timeout
            # The ismaster command is cheap and does not require auth.
            self.client.admin.command('ismaster')
            self.db = self.client[self.db_name]
            self.collection = self.db[self.collection_name]
            logger.info(f"Successfully connected to MongoDB: {self.mongo_uri}, using {self.db_name}/{self.collection_name}")
        except ConnectionFailure:
            logger.error(f"MongoDB connection failed to {self.mongo_uri}. Please check MongoDB instance and URI.", exc_info=True)
            # Decide how to handle this: raise error, or allow to run without DB (if applicable)
            # For a crawler, DB connection is usually critical.
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred during MongoDB connection: {e}", exc_info=True)
            raise

    @abstractmethod
    async def fetch_news_list(self, limit: int = 20) -> List[Dict[str, str]]:
        """
        Fetches a list of news article URLs and their metadata from the source.
        Each item in the list should be a dictionary, typically including 'url' and 'title'.
        Example: [{"url": "http://example.com/news1", "title": "News 1"}, ...]
        """
        pass

    @abstractmethod
    async def fetch_article_content(self, url: str) -> Optional[str]:
        """
        Fetches and parses the full content of a single news article from its URL.
        Should return the main textual content of the article.
        """
        pass

    async def _make_request(self, url: str, retries: int = 3) -> Optional[BeautifulSoup]:
        """
        Makes an HTTP GET request with retries and returns a BeautifulSoup object.
        Uses self.retry_delay for the delay between retries.
        Allows forcing response encoding via self.force_encoding.
        """
        last_exception = None
        for attempt in range(retries):
            try:
                response = await self.http_client.get(url)
                response.raise_for_status()  # Raises HTTPStatusError for 4xx/5xx responses
                
                # If a specific crawler knows the encoding is often misidentified,
                # it can set self.force_encoding.
                if self.force_encoding:
                    response.encoding = self.force_encoding
                    logger.debug(f"Forced encoding to {self.force_encoding} for {url}")
                
                # Ensure content is decoded correctly, httpx attempts to guess encoding.
                # Pass the determined encoding (either forced or detected by httpx) to BeautifulSoup.
                # response.encoding will be None if httpx couldn't determine it and we didn't force it.
                # BeautifulSoup will then try its own detection if from_encoding is None.
                current_encoding = response.encoding # This will be self.force_encoding if set, or httpx's guess
                return BeautifulSoup(response.content, 'html.parser', from_encoding=current_encoding)
            except httpx.TimeoutException as e:
                last_exception = e
                logger.warning(f"Timeout for {url} on attempt {attempt + 1}/{retries}. Retrying in {self.retry_delay}s...")
                await asyncio.sleep(self.retry_delay)
            except httpx.HTTPStatusError as e:
                last_exception = e
                logger.error(f"HTTP error {e.response.status_code} for {url}: {e.request.url}. Attempt {attempt + 1}/{retries}.")
                if e.response.status_code in [403, 404, 401, 514]: # Added 514 to non-retryable for immediate break if still capped after delay
                    logger.warning(f"Not retrying for {e.response.status_code} on {url} after this attempt, but will respect retry_delay.")
                    # We still sleep here because the next top-level attempt by the crawler might happen too soon otherwise.
                    await asyncio.sleep(self.retry_delay) 
                    break # Break from retries loop for these errors
                await asyncio.sleep(self.retry_delay)
            except httpx.RequestError as e: # Catches other httpx-related errors like network issues
                last_exception = e
                logger.error(f"Request error for {url}: {e}. Attempt {attempt + 1}/{retries}. Retrying in {self.retry_delay}s...")
                await asyncio.sleep(self.retry_delay)
            except Exception as e:
                last_exception = e
                logger.error(f"Unexpected error fetching {url}: {e}. Attempt {attempt + 1}/{retries}. Retrying in {self.retry_delay}s...", exc_info=True)
                await asyncio.sleep(self.retry_delay) # Wait before retrying on unexpected errors

        logger.error(f"Failed to fetch {url} after {retries} retries. Last error: {last_exception}")
        return None
    
    async def _make_raw_request(self, url: str, retries: int = 3) -> Optional[str]:
        """
        Makes an HTTP GET request with retries and returns the raw text content.
        Uses self.retry_delay for the delay between retries.
        Allows forcing response encoding via self.force_encoding.
        """
        last_exception = None
        for attempt in range(retries):
            try:
                response = await self.http_client.get(url)
                response.raise_for_status()  # Raises HTTPStatusError for 4xx/5xx responses
                
                if self.force_encoding:
                    response.encoding = self.force_encoding
                    logger.debug(f"Forced encoding to {self.force_encoding} for {url}")
                
                return response.text
            except httpx.TimeoutException as e:
                last_exception = e
                logger.warning(f"Timeout for {url} on attempt {attempt + 1}/{retries} (raw request). Retrying in {self.retry_delay}s...")
                await asyncio.sleep(self.retry_delay)
            except httpx.HTTPStatusError as e:
                last_exception = e
                logger.error(f"HTTP error {e.response.status_code} for {url} (raw request): {e.request.url}. Attempt {attempt + 1}/{retries}.")
                if e.response.status_code in [403, 404, 401, 514]:
                    logger.warning(f"Not retrying for {e.response.status_code} on {url} (raw request) after this attempt, but will respect retry_delay.")
                    await asyncio.sleep(self.retry_delay) 
                    break 
                await asyncio.sleep(self.retry_delay)
            except httpx.RequestError as e:
                last_exception = e
                logger.error(f"Request error for {url} (raw request): {e}. Attempt {attempt + 1}/{retries}. Retrying in {self.retry_delay}s...")
                await asyncio.sleep(self.retry_delay)
            except Exception as e:
                last_exception = e
                logger.error(f"Unexpected error fetching {url} (raw request): {e}. Attempt {attempt + 1}/{retries}. Retrying in {self.retry_delay}s...", exc_info=True)
                await asyncio.sleep(self.retry_delay)

        logger.error(f"Failed to fetch {url} (raw request) after {retries} retries. Last error: {last_exception}")
        return None

    async def run(self, limit: int = 20):
        """
        Main method to run the crawler: fetches news list, then article contents, and saves to DB.
        """
        logger.info(f"Running {self.__class__.__name__} to fetch up to {limit} articles.")
        try:
            news_items = await self.fetch_news_list(limit=limit)
            if not news_items:
                logger.info(f"No new news items found by {self.__class__.__name__}.")
                return

            articles_to_save = []
            processed_urls = set() # To avoid processing duplicate URLs from news list

            for item in news_items:
                # Fetch content only if the article is new or we need to update it (not implemented yet)
                article_url = item.get("url")
                if not article_url:
                    logger.warning(f"Skipping item with no URL: {item.get('title')}")
                    continue

                # Check if article already exists
                logger.debug(f"Checking DB for existing URL: {article_url}") # LOG URL being checked
                existing_article = None
                if self.collection is not None:
                    existing_article = self.collection.find_one({"url": article_url})
                
                if existing_article:
                    logger.info(f"Article already exists in DB, skipping content fetch: {article_url}") # LOG if found
                    # Potentially update existing article here if needed in the future
                    continue
                else: # LOG if not found
                    logger.debug(f"Article not found in DB, proceeding to fetch content for: {article_url}")

                if article_url in processed_urls:
                    logger.warning(f"Skipping item with duplicate URL: {article_url}")
                    continue
                processed_urls.add(article_url)

                logger.info(f"Fetching content for: {item.get('title') or article_url}")
                content = await self.fetch_article_content(article_url)

                if content:
                    article_data = {
                        "url": article_url,
                        "title": item.get("title", "N/A"),
                        "source_page_url": item.get("source_page_url"), # URL of the page where this link was found
                        "content": content,
                        "source": self.collection_name, # Or a more specific source name
                        "source_db": self.db_name,
                        "source_collection": self.collection_name,
                        "fetched_at": datetime.now(),
                        "published_date": item.get("published_date"), # Should be datetime object if available
                        "analyzed": False, # Mark as not analyzed initially
                        # Add any other metadata common to all articles
                    }
                    articles_to_save.append(article_data)
                else:
                    logger.warning(f"Could not fetch content for {article_url}. It will not be saved.")
            
            if articles_to_save:
                await self.save_articles(articles_to_save)
            else:
                logger.info(f"No new articles to save for {self.__class__.__name__}.")

        except Exception as e:
            logger.error(f"An error occurred during {self.__class__.__name__}.run(): {e}", exc_info=True)
        finally:
            logger.info(f"{self.__class__.__name__} run finished.")


    async def save_articles(self, articles: List[Dict]):
        """
        Saves a list of fetched articles to MongoDB using bulk operations for efficiency.
        Uses update_one with upsert=True to avoid duplicates based on URL.
        """
        if self.collection is None:
            logger.error("MongoDB collection not initialized. Cannot save articles.")
            return
        if not articles:
            logger.info("No articles to save.")
            return

        operations = []
        for article in articles:
            # Using 'url' as the unique identifier for an article.
            # If an article with this URL already exists, it will be updated.
            # If not, it will be inserted.
            operations.append(
                UpdateOne({"url": article["url"]}, {"$set": article}, upsert=True)
            )

        if not operations:
            logger.info("No operations to perform for saving articles.")
            return

        logger.info(f"Attempting to save/update {len(operations)} articles to {self.db_name}/{self.collection_name}.")
        try:
            result = self.collection.bulk_write(operations)
            logger.info(
                f"Bulk write to MongoDB successful: "
                f"{result.inserted_count} inserted, "
                f"{result.matched_count} matched, "
                f"{result.modified_count} modified, "
                f"{result.upserted_count} upserted."
            )
        except OperationFailure as e:
            logger.error(f"MongoDB bulk write operation failed: {e.details}", exc_info=True)
            # Optionally, you could try individual inserts here as a fallback,
            # or log which articles failed if `e.details` provides that info.
        except Exception as e:
            logger.error(f"An unexpected error occurred during MongoDB bulk write: {e}", exc_info=True)
            
    async def close(self):
        """Closes the httpx client and MongoDB client."""
        await self.http_client.aclose()
        if self.client:
            self.client.close()
        logger.info(f"{self.__class__.__name__} clients closed.")

# Example of how a concrete crawler might look (incomplete, for structure only)
# class ConcreteCrawler(BaseCrawler):
#     def __init__(self, mongo_uri: str = "mongodb://localhost:27017/"):
#         super().__init__(db_name="NewsDB", collection_name="ConcreteNews", mongo_uri=mongo_uri)
#         self.start_url = "http://example-news.com/latest"

#     async def fetch_news_list(self, limit: int = 20) -> List[Dict[str, str]]:
#         # Implementation specific to ConcreteNews website
#         # soup = await self._make_request(self.start_url)
#         # ... parse soup to find article links and titles ...
#         # return [{"url": "...", "title": "...", "published_date": ...}, ...]
#         logger.info(f"Fetching news list from {self.start_url} (limit {limit})")
#         await asyncio.sleep(1) # Simulate network request
#         return [
#             {"url": f"{self.start_url}/article1", "title": "Concrete Article 1", "published_date": datetime.now()},
#             {"url": f"{self.start_url}/article2", "title": "Concrete Article 2", "published_date": datetime.now()}
#         ][:limit]

#     async def fetch_article_content(self, url: str) -> Optional[str]:
#         # Implementation specific to ConcreteNews website
#         # soup = await self._make_request(url)
#         # ... parse soup to extract main article text ...
#         # return "This is the full article content..."
#         logger.info(f"Fetching article content from {url}")
#         await asyncio.sleep(0.5) # Simulate network request
#         return f"Full content of {url}. Lorem ipsum dolor sit amet."


async def main():
    # This main function is for demonstration and testing of the BaseCrawler.
    # You would typically call specific crawler implementations.
    # Configure logging for standalone script execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logger.info("BaseCrawler main demo started (will not run a real crawl).")
    # To test a concrete crawler, you would do:
    # crawler = ConcreteCrawler()
    # await crawler.run(limit=5)
    # await crawler.close()
    logger.info("BaseCrawler main demo finished.")

if __name__ == "__main__":
    # This part is for direct execution and testing of the base crawler logic,
    # but the BaseCrawler itself is abstract and won't run directly.
    # You should run concrete implementations of crawlers.
    asyncio.run(main()) 