import asyncio
import logging
from typing import List, Dict, Optional
from urllib.parse import urljoin
import re
import json
import sys
import os
from bs4 import BeautifulSoup

# Add project root to sys.path for direct execution
# This needs to be done before attempting to import local packages like crawlers or utils
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from crawlers.base_crawler import BaseCrawler
from utils.date_utils import parse_date_string_to_datetime

logger = logging.getLogger(__name__)

class CnstockCrawler(BaseCrawler):
    def __init__(self, mongo_uri: str = "mongodb://localhost:27017/"):
        super().__init__(db_name="Cnstock_Stock", collection_name="cnstock_news_roll", mongo_uri=mongo_uri)
        # Example: "http://news.cnstock.com/news/sns_yw/index.html" for important news
        # Or "http://ggjd.cnstock.com/gglist/search/qmtbbdj" for company announcements
        # Ensuring this file is re-evaluated by Python
        self.start_url = "https://www.cnstock.com/" # Reverted to original, accessible homepage
        self.base_url = "https://www.cnstock.com" # Align base_url with start_url scheme

    async def fetch_news_list(self, limit: int = 20) -> List[Dict[str, str]]:
        """Fetches list of news articles from cnstock.com."""
        logger.info(f"Fetching news list from {self.start_url} for {self.__class__.__name__}")
        news_items = []
        processed_urls = set()
        
        soup = await self._make_request(self.start_url)
        if not soup:
            logger.error(f"Failed to fetch main news page: {self.start_url}")
            return news_items

        # Selectors for links on the main page
        # (Keep existing selectors, can be refined later if needed)
        article_link_selectors = [
            ("a.index_MarqueeTitle__xe9or", "div.index_item__CjwH0"),
            ("div.index_bigImgWrap__cpolj a.index_bigImgBox__q_B_F", "div.index_title__2lWWJ"),
            ("div.index_topTabsWrap__kXcA8 a.index_smallImgBox__6DB5o", "div.index_name__FFKnS")
        ]
        
        main_page_links_to_process = []
        for selector, title_container_selector in article_link_selectors:
            selected_links = soup.select(selector)
            for link_element in selected_links:
                raw_url = link_element.get("href")
                if not raw_url: continue
                article_url = urljoin(self.base_url, raw_url)
                if article_url in processed_urls: continue
                
                article_title = (link_element.select_one(title_container_selector) or link_element).get_text(strip=True)
                if not article_title: article_title = link_element.get('title', '').strip()
                if not article_title: continue # Skip if no title

                main_page_links_to_process.append({
                    "url": article_url,
                    "title": article_title,
                    "source_page_url": self.start_url,
                    "is_topic_detail": "/topicDetail/" in article_url
                })
                processed_urls.add(article_url)

        logger.info(f"Found {len(main_page_links_to_process)} initial links from {self.start_url}.")

        for link_info in main_page_links_to_process:
            if len(news_items) >= limit: break

            current_url = link_info["url"]
            current_title = link_info["title"]

            if link_info["is_topic_detail"]:
                logger.info(f"Processing topicDetail page for more articles: {current_url}")
                topic_soup = await self._make_request(current_url)
                if topic_soup:
                    next_data_script = topic_soup.find("script", id="__NEXT_DATA__", type="application/json")
                    if next_data_script and next_data_script.string:
                        try:
                            next_data = json.loads(next_data_script.string)
                            # Path based on observed structure for topicDetail pages
                            articles_in_topic = []
                            page_props = next_data.get("props", {}).get("pageProps", {})
                            detail_data = page_props.get("detailData", {})
                            special_child_list = detail_data.get("specialChildList", [])
                            
                            for child_section in special_child_list:
                                page_info_list = child_section.get("pageInfo", {}).get("list", [])
                                for item in page_info_list:
                                    item_url_raw = item.get("link") or item.get("contId") # link or contId
                                    if not item_url_raw: continue
                                    # contId might need prefixing if it's just an ID
                                    if isinstance(item_url_raw, str) and item_url_raw.isdigit():
                                        item_url = urljoin(self.base_url, f"/commonDetail/{item_url_raw}") # Assuming commonDetail for IDs
                                    elif isinstance(item_url_raw, str) and item_url_raw.startswith("/"):
                                        item_url = urljoin(self.base_url, item_url_raw)
                                    elif isinstance(item_url_raw, str): # Potentially already a full URL
                                        item_url = item_url_raw
                                    else:
                                        continue

                                    if item_url in processed_urls: continue
                                    item_title = item.get("name") or item.get("title")
                                    pub_time_str = item.get("pubTime") # This is often relative like "18小时前" or a date "2024-07-01"
                                    
                                    published_date = parse_date_string_to_datetime(pub_time_str, silent=True, relative_to_today_if_time_only=True)

                                    if item_url and item_title:
                                        news_items.append({
                                            "url": item_url,
                                            "title": item_title,
                                            "published_date": published_date,
                                            "source_page_url": current_url # Source is the topicDetail page
                                        })
                                        processed_urls.add(item_url)
                                        logger.debug(f"Added from topic ({current_title}): {item_title} - {item_url}")
                                    if len(news_items) >= limit: break
                                if len(news_items) >= limit: break
                        except json.JSONDecodeError as e:
                            logger.warning(f"Failed to parse __NEXT_DATA__ from topic page {current_url}: {e}")
                    else:
                        logger.warning(f"No __NEXT_DATA__ script found on topic page {current_url}")
                else:
                    logger.warning(f"Failed to fetch topic page {current_url} to extract sub-articles.")
            else: # It's a regular article link from the main page
                # For non-topicDetail pages from the initial list, we don't have date info here
                # Date will be attempted during content fetch if not already present, or rely on DB check
                news_items.append({
                    "url": current_url,
                    "title": current_title,
                    "published_date": None, # Will be fetched or refined later if possible
                    "source_page_url": link_info["source_page_url"]
                })
            
            if len(news_items) >= limit: break

        if not news_items:
            logger.warning(f"Could not extract any news items from {self.start_url} or its topic pages.")
        else:
            logger.info(f"Fetched {len(news_items)} item(s) for content retrieval.")
        return news_items[:limit]

    async def fetch_article_content(self, url: str) -> Optional[str]:
        """Fetches and parses the full content of a single news article from cnstock.com."""
        logger.info(f"Fetching article content from: {url}")
        soup = await self._make_request(url)

        if not soup:
            logger.error(f"Failed to fetch article content from: {url}")
            return None

        # Strategy 1: Attempt to parse __NEXT_DATA__ for Next.js pages
        next_data_script = soup.find("script", id="__NEXT_DATA__", type="application/json")
        if next_data_script and next_data_script.string:
            try:
                next_data = json.loads(next_data_script.string)
                # Path for commonDetail pages: props.pageProps.data.textInfo.content
                # Path for topicDetail (though should be handled by fetch_news_list):
                # props.pageProps.detailData.summary or similar for description
                content_html = None
                page_props = next_data.get("props", {}).get("pageProps", {})
                
                if "/commonDetail/" in url or "/companies/" in url or "/news/" in url:
                    data_obj = page_props.get("data", {})
                    text_info = data_obj.get("textInfo", {})
                    content_html = text_info.get("content")
                    if not content_html and not text_info: # Check if `data` itself is the textInfo like object
                         content_html = data_obj.get("content") # some pages might have it directly under data
                    if not content_html: # Fallback to summary if content is missing
                        content_html = data_obj.get("summary")
                        if content_html: logger.info(f"Using 'summary' from __NEXT_DATA__ for {url} as 'content' was missing.")

                elif "/topicDetail/" in url: # Should ideally not be called directly for content
                    detail_data = page_props.get("detailData", {})
                    content_html = detail_data.get("summary") # Topic summary, not full article
                    if content_html: logger.info(f"Extracted summary for topicDetail page {url} from __NEXT_DATA__.")
                    else: logger.warning(f"topicDetail page {url} called in fetch_article_content, but no summary in __NEXT_DATA__.")
                    # For topicDetail, the real content is the list of articles, handled in fetch_news_list
                    # Returning summary or None is appropriate here as it's not a standard article body
                    if content_html:
                        content_soup = BeautifulSoup(content_html, "html.parser")
                        return content_soup.get_text(separator='\n', strip=True)
                    return None # No single article body for topic pages

                if content_html and isinstance(content_html, str):
                    logger.info(f"Successfully extracted HTML content from __NEXT_DATA__ for {url}")
                    # The content is HTML, so parse it with BeautifulSoup to get text
                    content_soup = BeautifulSoup(content_html, "html.parser")
                    
                    # Standard unwanted tag removal from this HTML snippet
                    for unwanted_tag_selector in ['script', 'style', 'iframe', 'figure', 'figcaption']:
                        for tag in content_soup.find_all(unwanted_tag_selector):
                            tag.decompose()
                    
                    text_content = content_soup.get_text(separator='\n', strip=True)
                    text_content = re.sub(r'\n{3,}', '\n\n', text_content) # Consolidate multiple newlines
                    if text_content.strip():
                        logger.debug(f"Content from __NEXT_DATA__ for {url} (len: {len(text_content)}): {text_content[:200]}...")
                        return text_content.strip()
                    else:
                        logger.warning(f"__NEXT_DATA__ content for {url} was empty after HTML parsing.")
                else:
                    logger.info(f"__NEXT_DATA__ found for {url}, but 'content' key missing or not string in expected path.")
            except json.JSONDecodeError as e:
                logger.warning(f"Error decoding __NEXT_DATA__ JSON for {url}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error processing __NEXT_DATA__ for {url}: {e}", exc_info=True)
        else:
            logger.info(f"__NEXT_DATA__ script not found for {url}. Falling back to other methods.")

        # Strategy 2: Attempt original ld+json (less likely for Next.js but keep as a fallback)
        try:
            ld_json_script = soup.find("script", type="application/ld+json")
            if ld_json_script and ld_json_script.string:
                data = json.loads(ld_json_script.string)
                if isinstance(data, list): data = data[0] if data else {}
                content_keys = ["articleBody", "description", "text"]
                content = next((data[key] for key in content_keys if data.get(key) and isinstance(data[key], str) and len(data[key]) > 20), None)
                if content:
                    logger.info(f"Successfully extracted content from ld+json for {url}")
                    parsed_content = BeautifulSoup(content, 'html.parser').get_text(separator='\n', strip=True)
                    if parsed_content.strip(): return parsed_content.strip()
        except Exception as e:
            logger.warning(f"Error processing ld+json for {url}: {e}", exc_info=False)

        logger.info(f"Falling back to direct HTML element parsing for {url} after __NEXT_DATA__ and ld+json attempts.")
        # Strategy 3: Direct HTML element parsing (original fallback)
        content_selectors = [
            ("div", {"class": "content-text"}), # Common class from other sites, maybe also here
            ("div", {"class": lambda x: x and "Detail_content" in x}), # From previous attempts for cnstock
            ("div", {"class": lambda x: x and "article_content" in x}),# Generic
            ("div", {"class": "article-body"}),
            ("div", {"id": "content_detail"}),
            ("article", {}),
            ("div", {"class": "TRS_Editor"}), # From other crawlers, worth a try
            ("main", {}), # Last resort, as it often contains too much
        ]
        
        content_div = None
        for tag_name, attrs in content_selectors:
            content_div = soup.find(tag_name, attrs)
            if content_div:
                selector_str = f"{tag_name}" + (f".{attrs.get('class')}" if attrs.get('class') and isinstance(attrs.get('class'), str) else "") + (f"#{attrs.get('id')}" if attrs.get('id') else "")
                logger.info(f"Found content container for {url} with HTML selector: {selector_str}")
                break
        
        if not content_div:
            logger.error(f"Failed to find main content container for article: {url} using all parsing strategies.")
            logger.warning(f"Final HTML snippet for {url} (content parsing failed):{str(soup.prettify())[:7000]}")
            return None

        for unwanted_selector in ['script', 'style', 'iframe', 'figure', 'figcaption', 'aside', 'footer', 'header', 'nav', '.advertisement', '.related-articles', '.share-buttons', '.comments-section']:
            for tag in content_div.select(unwanted_selector):
                tag.decompose()
        
        paragraphs = [p.get_text(strip=True) for p in content_div.find_all("p") if p.get_text(strip=True)]
        full_content = "\n\n".join(paragraphs)
        
        if not full_content.strip():
            logger.info(f"Extracted paragraph content by HTML selectors is empty for {url}. Trying full div text.")
            full_content = content_div.get_text(separator='\n', strip=True)
            if not full_content.strip():
                logger.warning(f"Still no content after fallback to full div text using HTML selectors for {url}.")
                return None # Return None if truly empty after all attempts
        
        full_content = re.sub(r'\n{3,}', '\n\n', full_content).strip()
        logger.debug(f"Successfully fetched and parsed content via HTML selectors for {url} (length: {len(full_content)})")
        return full_content

    async def test_specific_url(self, specific_url: str = "https://www.cnstock.com/topicDetail/447848"):
        """Runs the crawler for a single specific URL for detailed debugging."""
        logger.info(f"--- Starting test_specific_url for: {specific_url} ---")
        # self.client (httpx) and self.mongo_client are initialized in BaseCrawler.__init__
        # which is called by CnstockCrawler.__init__.
        # So, no need for an explicit connection check here if crawler is instantiated.

        content = await self.fetch_article_content(specific_url)
        if content:
            logger.info(f"--- Extracted content for {specific_url} (first 500 chars): ---")
            logger.info(f"{content[:500]}...")
            # Here you could also save it to a temporary file for inspection if very long
            # with open("temp_article_content.txt", "w", encoding="utf-8") as f:
            # f.write(content)
            # logger.info("Full extracted content saved to temp_article_content.txt")
        else:
            logger.warning(f"--- No content extracted for {specific_url} in test_specific_url ---")
        logger.info(f"--- Finished test_specific_url for: {specific_url} ---")
        # self.collection.insert_one(...) # Example if you wanted to save it


# Main execution for testing this specific crawler
async def main():
    # Setup basic logging for testing
    # sys.path modification moved to top of script
    logging.basicConfig(
        level=logging.DEBUG, # Set to DEBUG to see more details during testing
        format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    crawler = CnstockCrawler()
    test_url_for_cnstock = "https://www.cnstock.com/commonDetail/447841"
    try:
        # Standard run:
        # await crawler.run(limit=1) # Fetch list and then first article
        
        # Test specific URL directly:
        logger.info(f"--- TESTING SPECIFIC URL: {test_url_for_cnstock} ---")
        await crawler.test_specific_url(test_url_for_cnstock)

    except Exception as e:
        logger.error(f"Error during CnstockCrawler test: {e}", exc_info=True)
    finally:
        await crawler.close()

if __name__ == "__main__":
    asyncio.run(main()) 