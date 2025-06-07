import asyncio
import logging
from typing import List, Dict, Optional
from urllib.parse import urljoin
import re
import json # Added for ld+json parsing

from crawlers.base_crawler import BaseCrawler
from utils.date_utils import parse_date_string_to_datetime

logger = logging.getLogger(__name__)

class NbdCrawler(BaseCrawler):
    def __init__(self, mongo_uri: str = "mongodb://localhost:27017/"):
        super().__init__(db_name="Nbd_Stock", collection_name="nbd_news_roll", mongo_uri=mongo_uri)
        # National Business Daily (NBD) - http://www.nbd.com.cn/
        # Common news list page might be like: http://www.nbd.com.cn/columns/325 (for "快讯" - Flash News)
        # Or http://www.nbd.com.cn/columns/type/15 (for "股市直播" - Stock Market Live)
        self.start_url = "https://finance.nbd.com.cn/" # Updated to the financial news section
        self.base_url = "https://finance.nbd.com.cn/" # Ensure base_url matches new start_url

    async def fetch_news_list(self, limit: int = 20) -> List[Dict[str, str]]:
        logger.info(f"Fetching news list from {self.start_url} for {self.__class__.__name__}")
        news_items = []
        soup = await self._make_request(self.start_url)

        if not soup:
            logger.error(f"Failed to fetch main news page: {self.start_url}")
            return news_items

        # Adjust selectors based on nbd.com.cn's HTML structure.
        # The relevant news list appears to be within a div styled with overflow:hidden
        
        # Primary selector based on the observed HTML structure for "最新资讯"
        # The div with style="height:0;width:0;overflow:hidden" seems to reliably contain the news list.
        # However, directly selecting by style can be brittle. Let's hope it has other identifiers or is unique.
        # A safer bet might be to find the h3 with "最新资讯" and then get its sibling ul.
        # For now, let's try a direct path if the hidden div is consistent.

        # First, try to find the specific hidden div's ul
        # Looking for a div that likely contains the news, then its ul, then li, then a
        article_list_container = soup.find("div", style=lambda x: x and "overflow:hidden" in x and "height:0" in x)

        article_elements = []
        if article_list_container:
            # Find the <ul> that is a direct child or a descendant of this container
            news_ul = article_list_container.find('ul')
            if news_ul:
                article_elements = news_ul.select("li > a") # Get 'a' tags that are direct children of 'li'
                if article_elements:
                    logger.info(f"Successfully found article elements within the hidden div's ul.")
            else:
                logger.warning("Found the hidden div, but no 'ul' inside it.")
        
        if not article_elements:
            # Fallback to the previous list of selectors if the specific hidden div method fails
            logger.info("Hidden div method failed or found no articles, trying general selectors...")
            selectors_to_try = [
                "div.latest-news ul li a",
                "ul.news-list li a",
                "section.latest-updates ul li a",
                ".news_list li a",
                "div[class*='zx_list'] ul li a",
                "div.u-newslist ul.u-newslist-01 li a",
                "ul.u_news_list li div.news_title a", 
                "div.m-columnnewslist ul li a"
            ]
            for selector in selectors_to_try:
                article_elements = soup.select(selector)
                if article_elements:
                    logger.info(f"Successfully found article elements with fallback selector: '{selector}'")
                    break
        
        if not article_elements:
            logger.warning(f"Could not find article elements with any of the attempted selectors on {self.start_url}")
            if soup:
                logger.debug(f"Page source snippet for {self.start_url} (if no articles found):\n{str(soup.prettify())[:15000]}") # Reverted to DEBUG
            return news_items

        for elem_a in article_elements:
            if len(news_items) >= limit:
                break
            
            raw_url = elem_a.get("href")
            # Title can be from the nested span or the 'title' attribute of the <a> tag
            title_span = elem_a.find('span')
            article_title = title_span.get_text(strip=True) if title_span else elem_a.get('title', '').strip()

            if raw_url and article_title:
                article_url = urljoin(self.base_url, raw_url) if not raw_url.startswith('http') else raw_url
                
                # Date extraction for NBD (finance.nbd.com.cn)
                # The date is in a <span> that is a sibling of the <a> tag.
                date_str = None
                date_sibling_span = elem_a.find_next_sibling('span')
                if date_sibling_span:
                    date_str = date_sibling_span.get_text(strip=True)
                else:
                    # Fallback if direct sibling span is not found, try regex on parent li text (less reliable now)
                    parent_li = elem_a.find_parent('li')
                    if parent_li:
                        full_li_text = parent_li.get_text(separator=' ', strip=True)
                        import re
                        match = re.search(r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?)(\s+|$)', full_li_text)
                        if match:
                            date_str = match.group(1).strip()
                
                published_date = parse_date_string_to_datetime(date_str)

                news_items.append({
                    "url": article_url,
                    "title": article_title,
                    "published_date": published_date,
                    "source_page_url": self.start_url
                })
                logger.debug(f"Found article: {article_title} - {article_url} - {date_str}")
            else:
                logger.warning(f"Could not extract title or URL from element: {str(elem_a)[:100]}...")

        if not news_items:
            logger.warning(f"No news items extracted from {self.start_url}. Check CSS selectors.")
            # Log HTML snippet here as well, in case article_elements was found but parsing failed for all
            if soup and not article_elements: # Check if article_elements was indeed empty
                 # Temporarily log at WARNING
                 logger.debug(f"Re-logging page source for {self.start_url} as no items were extracted:\n{str(soup.prettify())[:15000]}") # Reverted to DEBUG
        else:
            logger.info(f"Fetched {len(news_items)} item(s) from {self.start_url}")
        return news_items[:limit]

    async def fetch_article_content(self, url: str) -> Optional[str]:
        logger.info(f"Fetching article content from: {url}")
        soup = await self._make_request(url)

        if not soup:
            logger.error(f"Failed to fetch article content from: {url}")
            return None

        # Attempt 1: Extract from <script type="application/ld+json">
        try:
            ld_json_script = soup.find("script", type="application/ld+json")
            if ld_json_script:
                data = json.loads(ld_json_script.string)
                if isinstance(data, list): # Sometimes ld+json is a list of objects
                    data = data[0] if data else {}
                
                content = data.get("articleBody") or data.get("description")
                if content and isinstance(content, str):
                    logger.info(f"Successfully extracted content from ld+json for {url}")
                    # Perform basic cleanup (optional, can be expanded)
                    content = re.sub(r'<[^>]+>', '', content) # Remove HTML tags if any
                    content = content.strip()
                    if content: # Ensure content is not empty after stripping
                        return content
                    else:
                        logger.info(f"ld+json content for {url} became empty after stripping.")
                else:
                    logger.info(f"ld+json found for {url}, but 'articleBody' or 'description' key was missing or content was not a non-empty string.")
            else:
                logger.info(f"No ld+json script found on {url}.")
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse ld+json for {url}.", exc_info=True)
        except Exception as e:
            logger.warning(f"Error processing ld+json for {url}: {e}", exc_info=True)

        logger.info(f"Falling back to HTML element parsing for {url} after ld+json attempt.")
        # --- SITE-SPECIFIC PARSING LOGIC FOR NBD.COM.CN (Fallback) ---
        content_container_selectors = [
            ("div", {"class": "g-articl-text"}),        # From analyzing HTML snippet for empty ld+json description
            ("div", {"class": "g_article_content"}),
            ("div", {"class": "article_content"}),
            ("div", {"class": "main_content"}),
            ("div", {"class": "u-article-content"}),
            ("div", {"class": "article-text"}),
            ("div", {"id": "articleText"}),
            ("article", {}),
            ("div", {"role": "article"}),
        ]
        
        content_div = None
        for tag_name, attrs in content_container_selectors:
            content_div = soup.find(tag_name, attrs)
            if content_div:
                selector_str = f"{tag_name}"
                if attrs.get("class"):
                    selector_str += f".{attrs.get('class')}"
                if attrs.get("id"):
                    selector_str += f"#{attrs.get('id')}"
                if attrs.get("role"):
                    selector_str += f"[role={attrs.get('role')}]"
                logger.info(f"Found content container for {url} with selector: {selector_str}")
                break
        
        if not content_div:
            logger.error(f"Failed to find main content container for article: {url} using HTML selectors. Check selectors.")
            logger.warning(f"HTML snippet for {url} (content parsing failed for both ld+json and HTML selectors):\n{str(soup.prettify())[:10000]}")
            return None

        # Remove unwanted elements
        for unwanted_tag in content_div.find_all(['script', 'style', 'div.ad', 'div.copyright', 'div.hot_news', 'div.share', 'div.tags', 'div.related_read', 'figure.ifengLogo', 'div.video-player', 'div.topLeftBar', 'div.author-info', 'div.article-function', 'div.comment-module']):
            unwanted_tag.decompose()
        
        paragraphs = [p.get_text(strip=True) for p in content_div.find_all("p")]
        full_content = "\n".join(paragraphs)
        
        if not full_content.strip():
            logger.warning(f"Extracted content from HTML elements is empty for {url}. Trying full div text.")
            full_content = content_div.get_text(separator='\n', strip=True)
            if not full_content.strip():
                logger.error(f"Still no content after fallback to full div text for {url}.")
                return None
        
        logger.debug(f"Successfully fetched and parsed content via HTML selectors for {url} (length: {len(full_content)})")
        return full_content

async def main():
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    crawler = NbdCrawler()
    try:
        await crawler.run(limit=5)
    except Exception as e:
        logger.error(f"Error during NbdCrawler test: {e}", exc_info=True)
    finally:
        await crawler.close()

if __name__ == "__main__":
    asyncio.run(main()) 