import aiohttp
import asyncio
import logging
from datetime import datetime
from typing import Optional, Dict, List
from bs4 import BeautifulSoup
import pandas as pd
from motor.motor_asyncio import AsyncIOMotorClient
from urllib.parse import urljoin
import json
import re

logger = logging.getLogger(__name__)

class EastmoneyMarketCrawler:
    def __init__(self, mongo_uri: str = "mongodb://localhost:27017"):
        self.mongo_uri = mongo_uri
        self.client = None
        self.db = None
        self.base_url = "https://data.eastmoney.com/stock/tradedetail.html"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

    async def connect(self):
        """Establish connection to MongoDB."""
        try:
            self.client = AsyncIOMotorClient(self.mongo_uri)
            self.db = self.client.stock_news
            logger.info("Connected to MongoDB successfully.")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    async def close(self):
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed.")

    async def fetch_dragon_tiger_list(self) -> Optional[pd.DataFrame]:
        """
        Fetches the Dragon & Tiger List data from Eastmoney API.
        Returns a DataFrame with the data or None if failed.
        Only fetches the first page (most relevant stocks, up to 20).
        """
        try:
            page_number = 1
            page_size = 20  # Only fetch the most relevant 20 stocks
            api_url = (
                f"https://datacenter-web.eastmoney.com/api/data/v1/get?"
                f"sortColumns=SECURITY_CODE,TRADE_DATE&sortTypes=-1,-1"
                f"&pageSize={page_size}&pageNumber={page_number}"
                f"&reportName=RPT_DAILYBILLBOARD_DETAILSNEW"
                f"&columns=SECURITY_CODE,SECUCODE,SECURITY_NAME_ABBR,TRADE_DATE,EXPLAIN,CLOSE_PRICE,CHANGE_RATE,"
                f"BILLBOARD_NET_AMT,BILLBOARD_BUY_AMT,BILLBOARD_SELL_AMT,BILLBOARD_DEAL_AMT,ACCUM_AMOUNT,"
                f"DEAL_NET_RATIO,DEAL_AMOUNT_RATIO,TURNOVERRATE,FREE_MARKET_CAP,EXPLANATION"
                f"&source=WEB&client=WEB"
            )
            async with aiohttp.ClientSession(headers=self.headers) as session:
                async with session.get(api_url) as response:
                    text = await response.text()
                    # Remove JSONP wrapper if present
                    match = re.search(r"^.*?\\((.*)\\);?$", text, re.DOTALL)
                    json_str = match.group(1) if match else text
                    data = json.loads(json_str)
                    if data.get("success") and "result" in data and "data" in data["result"]:
                        records = data["result"]["data"]
                        if not records:
                            logger.warning("No data found in Dragon & Tiger List API")
                            return None
                        df = pd.DataFrame(records)
                        df["fetch_time"] = datetime.now()
                        logger.info(f"Successfully fetched {len(df)} stocks from Dragon & Tiger List API (first page only)")
                        return df
                    else:
                        logger.error("API did not return expected data structure")
                        return None
        except Exception as e:
            logger.error(f"Error fetching Dragon & Tiger List from API: {e}", exc_info=True)
            return None

    def _safe_float(self, value: str) -> float:
        try:
            return float(value.replace(',', ''))
        except Exception:
            return 0.0

    async def save_to_mongodb(self, df: pd.DataFrame):
        """Save the Dragon & Tiger List data to MongoDB."""
        if df is None or df.empty:
            logger.warning("No data to save to MongoDB")
            return

        try:
            # Convert DataFrame to list of dictionaries
            records = df.to_dict('records')
            
            # Add timestamp for when the data was saved
            for record in records:
                record['saved_at'] = datetime.now()

            # Insert into MongoDB
            result = await self.db.dragon_tiger_list.insert_many(records)
            logger.info(f"Successfully saved {len(result.inserted_ids)} records to MongoDB")
        except Exception as e:
            logger.error(f"Error saving to MongoDB: {e}", exc_info=True)

    async def run(self, limit: int = None):
        """
        Main method to run the crawler.
        limit parameter is included for consistency with other crawlers but not used here
        as we want to fetch the complete Dragon & Tiger List.
        """
        try:
            await self.connect()
            
            # Fetch Dragon & Tiger List data
            df = await self.fetch_dragon_tiger_list()
            if df is not None:
                await self.save_to_mongodb(df)
            else:
                logger.error("Failed to fetch Dragon & Tiger List data")

        except Exception as e:
            logger.error(f"Error in crawler execution: {e}", exc_info=True)
        finally:
            await self.close()

# Example usage
async def main():
    logging.basicConfig(level=logging.INFO)
    crawler = EastmoneyMarketCrawler()
    await crawler.run()

if __name__ == "__main__":
    asyncio.run(main()) 