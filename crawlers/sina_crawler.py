import asyncio
import logging
from typing import List, Dict, Optional
from urllib.parse import urljoin
import json # For handling JSON data if Sina uses it for news lists
import datetime # For converting timestamp

from crawlers.base_crawler import BaseCrawler
from utils.date_utils import parse_date_string_to_datetime

logger = logging.getLogger(__name__)

class SinaCrawler(BaseCrawler):
    def __init__(self, mongo_uri: str = "mongodb://localhost:27017/"):
        super().__init__(db_name="Sina_Stock", collection_name="sina_news_roll", mongo_uri=mongo_uri)
        # Using the JSON API endpoint found, trying a different category
        self.start_url = "http://roll.finance.sina.com.cn/api/news_list.php?tag=2&cat_1=wlxbhd&cat_2=1" # Changed cat_1, added cat_2=1 as it was in the other one
        # Base URL for resolving relative article URLs if necessary (though API provides full URLs)
        self.base_url = "http://finance.sina.com.cn" 
        self.force_encoding = "gb2312" # API response seems to need gb2312 for its string escapes

    async def fetch_news_list(self, limit: int = 20) -> List[Dict[str, str]]:
        logger.info(f"Fetching news list from JSON API {self.start_url} for {self.__class__.__name__}")
        news_items = []
        
        raw_text_content = await self._make_raw_request(self.start_url)
            
        if not raw_text_content:
            logger.error(f"No content received from JSON API via _make_raw_request: {self.start_url}")
            return news_items

        # The response is like "var jsonData = {...};"
        # We need to strip the prefix and suffix to get valid JSON
        json_str = raw_text_content.strip()
        if json_str.startswith("var jsonData = "):
            json_str = json_str[len("var jsonData = "):]
        if json_str.endswith(";"):
            json_str = json_str[:-1]

        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON from {self.start_url}. Error: {e}")
            logger.error(f"Received text snippet: {json_str[:1000]}")
            return news_items

        if not isinstance(data, dict) or "list" not in data or not isinstance(data["list"], list):
            logger.error(f"JSON structure is not as expected from {self.start_url}. 'list' field missing or not a list.")
            logger.error(f"Received data structure: {str(data)[:1000]}")
            return news_items

        api_articles = data["list"]

        for item in api_articles:
            if len(news_items) >= limit:
                break

            article_title = item.get("title")
            article_url = item.get("url")
            timestamp = item.get("time") # This is a Unix timestamp

            if not (article_url and article_title and timestamp):
                logger.debug(f"Skipping item with missing URL, title, or time: {str(item)[:200]}")
                continue
            
            # Ensure URL is absolute (it should be from the API)
            if not article_url.startswith('http'):
                article_url = urljoin(self.base_url, article_url)

            try:
                # Convert Unix timestamp to datetime object
                published_date = datetime.datetime.fromtimestamp(int(timestamp), tz=datetime.timezone.utc)
            except ValueError:
                logger.warning(f"Could not parse timestamp '{timestamp}' for article: {article_title}")
                published_date = None
            except TypeError:
                logger.warning(f"Timestamp '{timestamp}' is not an int/float for article: {article_title}")
                published_date = None


            news_items.append({
                "url": article_url,
                "title": article_title,
                "published_date": published_date,
                "source_page_url": self.start_url 
            })
            logger.debug(f"Found article via API: {article_title} - {article_url} - Date: {published_date}")

        if not news_items:
            logger.warning(f"No news items extracted from JSON API {self.start_url}, though API call might have succeeded.")
        else:
            logger.info(f"Fetched {len(news_items)} item(s) from JSON API {self.start_url}")
        return news_items[:limit]

    async def fetch_article_content(self, url: str) -> Optional[str]:
        logger.info(f"Fetching article content from: {url}")
        soup = await self._make_request(url)

        if not soup:
            logger.error(f"Failed to fetch article content from: {url}")
            return None

        # --- SITE-SPECIFIC PARSING LOGIC FOR SINA FINANCE ---
        # Adjust selectors for the main content area of Sina articles.
        # Common IDs/classes: "artibody", "article", "mainContent", "article-body"
        content_div = soup.find("div", id="artibody")
        if not content_div:
            content_div = soup.find("div", class_="article_content") # Common class name
        if not content_div:
            content_div = soup.find("div", class_="article-content") # another variation
        if not content_div:
            content_div = soup.find("div", class_="article") 
        if not content_div:
            content_div = soup.find("div", id="articleContent")
        if not content_div:
            content_div = soup.find("section", class_="art_pic_card art_content") # More modern semantic tag
        if not content_div:
            logger.error(f"Failed to find main content container for article: {url}. Check selectors. Dumping soup to investigate:")
            logger.error(soup.prettify()[:2000]) # Log first 2k chars of prettified soup
            return None

        # Remove unwanted elements
        for unwanted_tag in content_div.find_all(['script', 'style', 'div.appendQr_wrap', 'div.ggcontent', 'div.page-view', 'p.show_author']): # Add more as needed
            unwanted_tag.decompose()
        
        paragraphs = [p.get_text(strip=True) for p in content_div.find_all("p")]
        full_content = "\n".join(paragraphs)

        if not full_content.strip():
            logger.warning(f"Extracted content is empty for {url}. Trying full div text.")
            full_content = content_div.get_text(separator='\n', strip=True)
            if not full_content.strip():
                logger.error(f"Still no content after fallback for {url}.")
                return None

        logger.debug(f"Successfully fetched and parsed content for {url} (length: {len(full_content)})")
        return full_content

async def main():
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    crawler = SinaCrawler()
    try:
        await crawler.run(limit=5)
    except Exception as e:
        logger.error(f"Error during SinaCrawler test: {e}", exc_info=True)
    finally:
        await crawler.close()

if __name__ == "__main__":
    asyncio.run(main()) 