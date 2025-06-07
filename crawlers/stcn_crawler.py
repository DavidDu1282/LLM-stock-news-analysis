import asyncio
import logging
from typing import List, Dict, Optional
from urllib.parse import urljoin
import re

from crawlers.base_crawler import BaseCrawler
from utils.date_utils import parse_date_string_to_datetime

logger = logging.getLogger(__name__)

class StcnCrawler(BaseCrawler):
    def __init__(self, mongo_uri: str = "mongodb://localhost:27017/"):
        super().__init__(db_name="Stcn_Stock", collection_name="stcn_news_roll", mongo_uri=mongo_uri)
        # Securities Times (证券时报网) - www.stcn.com
        # Common news list: http://kuaixun.stcn.com/ (for "快讯" - Flash News)
        # Or company news: http://company.stcn.com/gsxw
        self.start_url = "https://www.stcn.com/article/list/yw.html" # Changed to "Important News" section
        self.base_url = "https://www.stcn.com" # Updated base URL for the main site
        self.force_encoding = None # Main site is likely UTF-8, allow auto-detection

    async def fetch_news_list(self, limit: int = 20) -> List[Dict[str, str]]:
        logger.info(f"Fetching news list from {self.start_url} for {self.__class__.__name__}")
        news_items = []
        soup = await self._make_request(self.start_url)

        if not soup:
            logger.error(f"Failed to fetch main news page: {self.start_url}")
            return news_items

        # New selector based on HTML snippet from yw.html page
        # Each news item is an <li> element within <ul class="list infinite-list">
        article_elements = soup.select("ul.list.infinite-list > li")
        
        if not article_elements:
            logger.warning(f"Could not find article list items with selector 'ul.list.infinite-list > li' on {self.start_url}.")
            logger.warning(f"Page source snippet for {self.start_url} (first 15000 chars):\n{str(soup.prettify())[:15000]}")
            return news_items
        
        logger.info(f"Found {len(article_elements)} potential list items.")

        for item_li in article_elements:
            if len(news_items) >= limit:
                break
            
            title_tag = item_li.select_one("div.tt > a") 
            
            if title_tag and title_tag.get("href"):
                raw_url = title_tag.get("href")
                article_title = title_tag.get_text(strip=True)
                
                article_url = urljoin(self.base_url, raw_url) # hrefs like /article/detail/1851841.html

                date_str = None
                published_date = None
                info_div = item_li.select_one("div.info")
                if info_div:
                    # Time is usually the last or second to last span. 
                    # Example: <span>作者</span> <span>11:34</span> or <span>来源</span> <span>作者</span> <span>11:11</span>
                    # We need to find the span that contains time like HH:MM
                    time_spans = info_div.find_all("span")
                    for span in reversed(time_spans): # Check from the end
                        potential_time_str = span.get_text(strip=True)
                        # Regex for HH:MM or H:MM
                        if potential_time_str and re.match(r"^\d{1,2}:\d{2}$", potential_time_str):
                            date_str = potential_time_str
                            break # Found the time
                
                if date_str:
                    # relative_to_today_if_time_only=True will make it assume current day if only time is found
                    published_date = parse_date_string_to_datetime(date_str, relative_to_today_if_time_only=True, silent=True)
                else:
                    logger.debug(f"Could not find date/time string for: {article_title}")

                news_items.append({
                    "url": article_url,
                    "title": article_title,
                    "published_date": published_date,
                    "source_page_url": self.start_url
                })
                logger.debug(f"Found article: {article_title} - {article_url} - Date: {published_date or 'N/A'} (Raw: {date_str or 'N/A'})")
            else:
                logger.debug(f"Could not extract title or URL from list item: {str(item_li)[:200]}...")
        
        if not news_items:
            logger.warning(f"No news items successfully extracted from {self.start_url} though {len(article_elements)} list items were found. Check item parsing logic.")
            if article_elements: # Log first few items if parsing failed
                logger.warning(f"HTML of first few list items (up to 3, 1000 chars each) for {self.start_url}:")
                for i, el in enumerate(article_elements[:3]):
                    logger.warning(f"Item {i+1}:\n{str(el.prettify())[:1000]}")
        else:
            logger.info(f"Fetched {len(news_items)} item(s) from {self.start_url}")
        return news_items[:limit]

    async def fetch_article_content(self, url: str) -> Optional[str]:
        logger.info(f"Fetching article content from: {url}")
        soup = await self._make_request(url)

        if not soup:
            logger.error(f"Failed to fetch article content from: {url}")
            return None

        # --- SITE-SPECIFIC PARSING LOGIC FOR STCN.COM ---
        # Adjust selectors for the main content area of STCN articles.
        content_div = None
        
        # Forcing check for "div.detail-content" based on user direct inspection
        logger.info(f"Attempting to find specific selector: div.detail-content for {url}")
        content_div = soup.find("div", class_="detail-content")

        if not content_div: 
            logger.error(f"Failed to find user-specified 'div.detail-content' for article: {url}.")
            logger.warning(f"Page source snippet for {url} (first 10000 chars):\n{str(soup.prettify())[:10000]}")
            # Fallback to the previously problematic selector to see if it's still there, for comparison
            fallback_div = soup.find("div", class_="content")
            if fallback_div:
                logger.warning(f"The old selector 'div.content' IS present. HTML (first 2000 chars):\n{str(fallback_div.prettify())[:2000]}")
            else:
                logger.warning(f"The old selector 'div.content' is ALSO NOT present.")
            return None
        else:
            # Log the HTML of the found content_div
            logger.info(f"Successfully found 'div.detail-content' for {url}. HTML (first 2000 chars):\n{str(content_div.prettify())[:2000]}")

        # Remove unwanted elements
        for unwanted_tag in content_div.find_all(['script', 'style', 'div.ad', 'div.statement', 'div.tjyd', 'div.zebian']): # Add more as needed
            unwanted_tag.decompose()

        paragraphs = [p.get_text(strip=True) for p in content_div.find_all("p")]
        full_content = "\n".join(paragraphs)

        if not full_content.strip():
            logger.warning(f"Extracted content from <p> tags is empty for {url}. Trying full div text.")
            full_content = content_div.get_text(separator='\n', strip=True)
            logger.info(f"Fallback content for {url} (len: {len(full_content)}): '{full_content[:500]}...'") # Log retrieved fallback content
            if not full_content.strip():
                logger.error(f"Still no content after fallback for {url}.")
                # Log HTML snippet here as well if even the fallback fails with an initially found container
                logger.warning(f"Fallback content extraction also failed. HTML of content_div for {url} (first 2000 chars):\n{str(content_div.prettify())[:2000]}")
                return None
        
        logger.debug(f"Successfully fetched and parsed content for {url} (length: {len(full_content)})")
        return full_content

async def main():
    logging.basicConfig(
        # level=logging.INFO, 
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    crawler = StcnCrawler()
    try:
        await crawler.run(limit=5)
    except Exception as e:
        logger.error(f"Error during StcnCrawler test: {e}", exc_info=True)
    finally:
        await crawler.close()

if __name__ == "__main__":
    asyncio.run(main()) 