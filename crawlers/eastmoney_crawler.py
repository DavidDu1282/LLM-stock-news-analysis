import asyncio
import logging
from typing import List, Dict, Optional
from urllib.parse import urljoin
import re
import json
import time
from datetime import datetime

from bs4 import BeautifulSoup

from crawlers.base_crawler import BaseCrawler
from utils.date_utils import parse_date_string_to_datetime

logger = logging.getLogger(__name__)

class EastmoneyCrawler(BaseCrawler):
    def __init__(self, mongo_uri: str = "mongodb://localhost:27017/"):
        super().__init__(db_name="Eastmoney_Stock", collection_name="eastmoney_kuaixun_api_news", mongo_uri=mongo_uri)
        self.api_url_template = "https://newsapi.eastmoney.com/kuaixun/v2/api/list?callback=ajaxResult_102&column=102&limit={limit}&p={page}&_={timestamp}"
        self.base_url = "https://finance.eastmoney.com/" 
        self.force_encoding = "gbk"
        logger.info(f"EastmoneyCrawler configured for Kuaixun API: {self.api_url_template}")

    def _strip_jsonp_wrapper(self, jsonp_string: str, callback_name: str = "ajaxResult_102") -> Optional[str]:
        match = re.match(rf"^\s*{re.escape(callback_name)}\s*\((.*)\)\s*;?$\s*", jsonp_string, re.DOTALL)
        if match:
            return match.group(1).strip()
        
        match_fallback = re.match(rf"^\s*kxall_ajaxResult102\s*\((.*)\)\s*;?$\s*", jsonp_string, re.DOTALL)
        if match_fallback:
            logger.debug("Used fallback callback 'kxall_ajaxResult102' to strip JSONP wrapper.")
            return match_fallback.group(1).strip()

        logger.warning(f"Could not strip JSONP wrapper with primary callback '{callback_name}' or fallback 'kxall_ajaxResult102'. String: {jsonp_string[:200]}...")
        return None

    async def fetch_news_list(self, limit: int = 20, page: int = 1) -> List[Dict[str, str]]:
        timestamp = int(time.time() * 1000)
        api_url = self.api_url_template.format(limit=limit, page=page, timestamp=timestamp)
        
        logger.info(f"Fetching Kuaixun API news list from {api_url}")
        news_items = []
        
        original_force_encoding = self.force_encoding
        self.force_encoding = None 
        jsonp_response = await self._make_raw_request(api_url)
        self.force_encoding = original_force_encoding

        if not jsonp_response:
            logger.error(f"Failed to fetch Kuaixun API response from: {api_url}")
            return news_items

        json_string = self._strip_jsonp_wrapper(jsonp_response)
        if not json_string:
            logger.error(f"Failed to parse JSONP response from {api_url}. Response snippet: {jsonp_response[:500]}")
            return news_items

        try:
            data = json.loads(json_string)
        except json.JSONDecodeError as e:
            logger.error(f"JSON decoding failed for Kuaixun API response: {e}. String: {json_string[:500]}...")
            return news_items

        if data.get("rc") != 1 or not data.get("news"):
            logger.warning(f"Kuaixun API response indicates failure or no news. RC: {data.get('rc')}, ME: {data.get('me')}. URL: {api_url}")
            return news_items

        api_news_list = data.get("news", [])
        logger.info(f"Received {len(api_news_list)} items from Kuaixun API.")
        
        processed_urls = set()
        for item_data in api_news_list:
            if len(news_items) >= limit:
                break
            
            article_title = item_data.get("title")
            article_url = item_data.get("url_w")
            show_time_str = item_data.get("showtime")

            if not article_title or not article_url or not show_time_str:
                logger.debug(f"Skipping API item with missing title, url, or showtime: {item_data}")
                continue
            
            if article_url in processed_urls:
                logger.debug(f"Skipping duplicate Kuaixun API URL: {article_url}")
                continue

            published_date = parse_date_string_to_datetime(show_time_str, formats=["%Y-%m-%d %H:%M:%S"], silent=True)
            if not published_date:
                 published_date = parse_date_string_to_datetime(show_time_str, formats=["%Y-%m-%d %H:%M"], silent=True)

            news_items.append({
                "url": article_url,
                "title": article_title,
                "published_date": published_date,
                "source_page_url": api_url
            })
            processed_urls.add(article_url)
            logger.debug(f"Processed Kuaixun API item: {article_title} - {article_url} - Date: {published_date}")

        if not news_items:
            logger.warning(f"No Kuaixun items successfully processed from API response {api_url} though {len(api_news_list)} items were received.")
        else:
            logger.info(f"Fetched {len(news_items)} Kuaixun item(s) from API {api_url}")
        return news_items[:limit]

    async def fetch_article_content(self, url: str) -> Optional[str]:
        logger.info(f"Fetching article content from: {url} (using encoding: {self.force_encoding or 'auto'})")
        soup = await self._make_request(url) 

        if not soup:
            logger.error(f"Failed to fetch article content from: {url}")
            return None

        content_div = None
        selectors = [
            ("div", {"id": "ContentBody"}),      
            ("div", {"class": "newsContent"}),   
            ("div", {"class": "Body"}),          
            ("article", {"class": "content-text"}), 
            ("div", {"class": "content_body"}),
            ("div", {"class": "article-content"}),
        ]

        for tag_name, attrs in selectors:
            content_div = soup.find(tag_name, attrs)
            if content_div:
                logger.debug(f"Found content container with selector: ({tag_name}, {attrs}) for {url}")
                break
        
        if not content_div:
            logger.warning(f"Failed to find main content container for article: {url}. Check selectors.")
            logger.warning(f"HTML snippet for {url} (first 5000 chars):\n{str(soup.prettify())[:5000]}")
            return None

        for unwanted_selector in [
            "script", "style", 
            "div.c_review_comment", "div.c-comment-circle", 
            "div.footer_copyright", "div.footer", 
            "div.share-bar", "div.share_buttons", 
            "div.readall", "div.readall_box", 
            "div.ad", "div.gg", "[class*='advert']", 
            "div.corrnews", "div.related_news", 
            "div.source", "p.source", "span.source", 
            "p.detail-link", 
            "div#em_stock_comments", 
            "div.hide", "[style*='display:none']", 
            "iframe"
        ]:
            for tag in content_div.select(unwanted_selector):
                tag.decompose()
        
        full_content = content_div.get_text(separator='\n', strip=True)
        full_content = re.sub(r'\n{3,}', '\n\n', full_content)

        if not full_content.strip():
            logger.warning(f"Extracted content is empty for {url} after trying selectors and get_text().")
            logger.warning(f"Content_div HTML for {url}:\n{str(content_div.prettify())[:3000]}")
            return None

        logger.debug(f"Successfully fetched and parsed content for {url} (length: {len(full_content)})")
        return full_content.strip()

async def main():
    logging.basicConfig(
        level=logging.DEBUG, 
        format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    crawler = EastmoneyCrawler()
    try:
        logger.info("Starting EastmoneyCrawler (Kuaixun API) test run...")
        await crawler.run(limit=5) 
    except Exception as e:
        logger.error(f"Error during EastmoneyCrawler (Kuaixun API) test: {e}", exc_info=True)
    finally:
        await crawler.close()
        logger.info("EastmoneyCrawler (Kuaixun API) test run finished.")

if __name__ == "__main__":
    asyncio.run(main()) 