import asyncio
import logging
from typing import List, Dict, Optional
from urllib.parse import urljoin

from crawlers.base_crawler import BaseCrawler
from utils.date_utils import parse_date_string_to_datetime

logger = logging.getLogger(__name__)

class JrjCrawler(BaseCrawler):
    def __init__(self, mongo_uri: str = "mongodb://localhost:27017/"):
        super().__init__(db_name="Jrj_Stock", collection_name="jrj_news_roll", mongo_uri=mongo_uri)
        # JRJ Finance: finance.jrj.com.cn
        # Current news list: http://finance.jrj.com.cn/list/industrynews.shtml
        self.start_url = "http://finance.jrj.com.cn/list/industrynews.shtml"
        self.base_url = "http://finance.jrj.com.cn" # Base URL for finance section
        self.retry_delay = 60 # Override retry_delay for JRJ to 60 seconds

    async def fetch_news_list(self, limit: int = 20) -> List[Dict[str, str]]:
        logger.info(f"Fetching news list from {self.start_url} for {self.__class__.__name__}")
        news_items = []
        soup = await self._make_request(self.start_url)

        if not soup:
            logger.error(f"Failed to fetch main news page: {self.start_url}")
            return news_items

        # --- SITE-SPECIFIC PARSING LOGIC FOR JRJ.COM.CN ---
        # Adjust selectors based on jrj.com.cn's HTML structure.
        article_elements = soup.select("div.list-main ul li") # Main list container
        if not article_elements:
            article_elements = soup.select("div.listmain ul li") # Alternative container
            if not article_elements:
                logger.warning(f"Could not find article elements with primary or fallback selectors on {self.start_url}")

        for elem in article_elements:
            if len(news_items) >= limit:
                break
            
            title_tag = elem.find("a")
            date_tag = elem.find("span", class_="time")
            
            if title_tag and title_tag.get("href"):
                raw_url = title_tag.get("href")
                article_title = title_tag.get_text(strip=True)
                
                # JRJ links can be absolute or relative. Ensure they are absolute.
                if raw_url.startswith("//"):
                    article_url = "http:" + raw_url
                elif not raw_url.startswith('http'):
                    article_url = urljoin(self.base_url, raw_url)
                else:
                    article_url = raw_url

                # Date parsing for JRJ
                date_str = date_tag.get_text(strip=True) if date_tag else None
                published_date = parse_date_string_to_datetime(date_str)

                news_items.append({
                    "url": article_url,
                    "title": article_title,
                    "published_date": published_date,
                    "source_page_url": self.start_url
                })
                logger.debug(f"Found article: {article_title} - {article_url} - {date_str}")
            else:
                logger.warning(f"Could not extract title or URL from element: {str(elem)[:100]}...")
        
        if not news_items:
            logger.warning(f"No news items extracted from {self.start_url}. Check CSS selectors.")
        else:
            logger.info(f"Fetched {len(news_items)} item(s) from {self.start_url}")
        return news_items[:limit]

    async def fetch_article_content(self, url: str) -> Optional[str]:
        logger.info(f"Fetching article content from: {url}")
        soup = await self._make_request(url)

        if not soup:
            logger.error(f"Failed to fetch article content from: {url}")
            return None

        # --- SITE-SPECIFIC PARSING LOGIC FOR JRJ.COM.CN ---
        # Adjust selectors for the main content area of JRJ articles.
        # Example: Common class names are "texttit_m1" or "article-content".
        content_div = soup.find("div", class_="texttit_m1")
        if not content_div: # Fallback selector
            content_div = soup.find("div", class_="article-content")
            if not content_div:
                content_div = soup.find("div", class_="textmain") # Another common one
                if not content_div:
                    logger.error(f"Failed to find main content container for article: {url}. Check selectors.")
                    return None
        
        # Remove unwanted elements (ads, related links, scripts, styles)
        for unwanted_tag in content_div.find_all(['script', 'style', 'div.bottomAdd', 'div.pages']): # Add more as needed
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
    crawler = JrjCrawler()
    try:
        await crawler.run(limit=5)
    except Exception as e:
        logger.error(f"Error during JrjCrawler test: {e}", exc_info=True)
    finally:
        await crawler.close()

if __name__ == "__main__":
    asyncio.run(main()) 