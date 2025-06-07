import os
import sys # Add sys import
import logging

# Add project root to sys.path to allow for correct module imports
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, PROJECT_ROOT)

# Initialize logger earlier
logger = logging.getLogger(__name__)

import asyncio
import time
import finnhub
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from config import settings
from utils.market_utils import update_sp500_csv, DEFAULT_SP500_CSV_PATH_FOR_UTILS

# Assuming sp500_tickers.csv is in the project root, relative to where this script might be run from
# or that PYTHONPATH is set up correctly.
# For robustness, consider making path handling more explicit if issues arise.
try:
    from market_data.alpha_vantage_adapter import load_symbols_from_csv, DEFAULT_SP500_CSV_PATH
except ImportError:
    # Fallback if run directly or alpha_vantage_adapter is not found in the same way
    # This is a simplification; robust path handling would be better.
    logger.warning("Could not import load_symbols_from_csv from alpha_vantage_adapter. Defining a local version.")
    DEFAULT_SP500_CSV_PATH = "sp500_tickers.csv"
    def load_symbols_from_csv(csv_path: str = DEFAULT_SP500_CSV_PATH) -> List[str]:
        try:
            df = pd.read_csv(csv_path)
            if "Symbol" not in df.columns:
                # Use logging instead of logger if logger might not be defined yet in this exact scope
                logging.error(f"'Symbol' column not found in {csv_path}")
                return []
            symbols = df["Symbol"].dropna().unique().tolist()
            # Use logging instead of logger
            logging.info(f"Loaded {len(symbols)} unique symbols from {csv_path}")
            return symbols
        except FileNotFoundError:
            # Use logging instead of logger
            logging.warning(f"Ticker CSV file not found at {csv_path}. Returning empty list.")
            return []
        except Exception as e:
            # Use logging instead of logger
            logging.error(f"Error loading symbols from {csv_path}: {e}", exc_info=True)
            return []


FINNHUB_API_KEY = settings.FINNHUB_API_KEY

class FinnhubAdapter:
    def __init__(self, api_key: str = None, sp500_csv_path: str = DEFAULT_SP500_CSV_PATH, update_sp500: bool = False):
        self.api_key = api_key if api_key else FINNHUB_API_KEY
        if not self.api_key:
            logger.error("Finnhub API key not provided. Please set FINNHUB_API_KEY in config.settings or pass during instantiation.")
            raise ValueError("Finnhub API key is required.")
        
        self.client = finnhub.Client(api_key=self.api_key)
        self.sp500_csv_path = sp500_csv_path
        
        # Update S&P 500 list if requested
        if update_sp500:
            logger.info("Updating S&P 500 tickers list from Wikipedia...")
            if update_sp500_csv(sp500_csv_path):
                logger.info("Successfully updated S&P 500 tickers list.")
            else:
                logger.warning("Failed to update S&P 500 tickers list. Will use existing CSV if available.")
        
        self.sp500_symbols = load_symbols_from_csv(sp500_csv_path)
        if not self.sp500_symbols:
            logger.warning("No S&P 500 symbols loaded for FinnhubAdapter. Ensure sp500_tickers.csv is present and correct.")
        else:
            logger.info(f"FinnhubAdapter initialized with {len(self.sp500_symbols)} S&P 500 symbols from {sp500_csv_path}.")

    async def get_daily_snapshot_quote(self, symbol: str) -> Optional[pd.DataFrame]:
        """
        Fetches the latest available daily quote (OHLC, previous close) for a symbol.
        Uses the /quote endpoint.
        Returns a single-row DataFrame.
        """
        try:
            loop = asyncio.get_running_loop()
            # client.quote is synchronous
            quote_data = await loop.run_in_executor(None, self.client.quote, symbol)
            
            if not quote_data or quote_data.get('c') is None: # Check if current price is missing, indicating no valid data
                logger.warning(f"No valid quote data returned for {symbol} from Finnhub. Response: {quote_data}")
                return None

            # Convert to DataFrame
            # Timestamp 't' is for when the quote was generated. 
            # For EOD, this usually represents the market close day.
            df = pd.DataFrame({
                'open': [quote_data.get('o')],
                'high': [quote_data.get('h')],
                'low': [quote_data.get('l')],
                'close': [quote_data.get('c')], # Current price, effectively close for EOD
                'previous_close': [quote_data.get('pc')],
                'timestamp': [pd.to_datetime(quote_data.get('t'), unit='s')]
            })
            df.set_index('timestamp', inplace=True)
            logger.info(f"Successfully fetched daily snapshot quote for {symbol} from Finnhub.")
            return df
        except finnhub.FinnhubAPIException as e:
            if e.status_code == 403:
                 logger.error(f"Finnhub API access forbidden (403) for quote on {symbol}: {e}. This endpoint might also be restricted for your key.")
            elif e.status_code == 429: 
                 logger.error(f"Finnhub API rate limit hit (429) for quote on {symbol}: {e}")
            else:
                 logger.error(f"Finnhub API exception for quote on {symbol}: {e} (Status: {e.status_code})")
            return None
        except Exception as e:
            logger.error(f"Error fetching daily snapshot quote for {symbol} from Finnhub: {e}", exc_info=True)
            return None

    async def get_latest_daily_quotes_for_sp500(self) -> Dict[str, Optional[pd.DataFrame]]:
        """
        Fetches the latest daily snapshot quote for all loaded S&P 500 symbols.
        """
        if not self.sp500_symbols:
            logger.warning("S&P 500 symbol list is empty. Cannot fetch quotes.")
            return {}

        results: Dict[str, Optional[pd.DataFrame]] = {}
        logger.info(f"Fetching latest daily snapshot quotes for {len(self.sp500_symbols)} S&P 500 symbols...")
        
        for i, symbol in enumerate(self.sp500_symbols):
            logger.info(f"Fetching quote for {symbol} ({i+1}/{len(self.sp500_symbols)})...")
            quote_df = await self.get_daily_snapshot_quote(symbol)
            results[symbol] = quote_df
            
            if (i + 1) < len(self.sp500_symbols):
                await asyncio.sleep(1.1) # Respect 60 calls/minute limit
        
        logger.info(f"Finished fetching quotes for S&P 500. Results acquired for {sum(1 for df in results.values() if df is not None)} symbols.")
        return results
    
    async def get_news_sentiment(self, symbol: str) -> Optional[List[Dict[str, Any]]]:
        """
        Fetches news sentiment for a specific stock symbol.
        Note: Finnhub's news sentiment endpoint is usually by symbol.
        API: https://finnhub.io/docs/api/news-sentiment
        Free plan usually allows this for major stocks.
        """
        try:
            loop = asyncio.get_running_loop()
            news_sentiment = await loop.run_in_executor(None, self.client.news_sentiment, symbol)
            logger.info(f"Fetched news sentiment for {symbol}. Count: {len(news_sentiment) if news_sentiment else 0}")
            return news_sentiment
        except finnhub.FinnhubAPIException as e:
            if e.status_code == 403:
                logger.error(f"Finnhub API access forbidden (403) for news sentiment on {symbol}: {e}.")
            elif e.status_code == 429:
                 logger.error(f"Finnhub API rate limit hit (429) for news sentiment on {symbol}: {e}")
            else:
                 logger.error(f"Finnhub API exception for news sentiment on {symbol}: {e} (Status: {e.status_code})")
            return None
        except Exception as e:
            logger.error(f"Error fetching news sentiment for {symbol} from Finnhub: {e}", exc_info=True)
            return None

    async def get_insider_sentiment(self, symbol: str, from_date: str, to_date: str) -> Optional[Dict[str, Any]]:
        """
        Fetches insider sentiment data for a given symbol and date range.
        from_date, to_date: YYYY-MM-DD string format.
        Returns the raw JSON response which includes a 'data' list of monthly sentiment and the symbol.
        """
        try:
            loop = asyncio.get_running_loop()
            # client.stock_insider_sentiment is synchronous
            sentiment_data = await loop.run_in_executor(
                None, 
                self.client.stock_insider_sentiment, 
                symbol, 
                from_date, 
                to_date
            )
            
            if not sentiment_data or not sentiment_data.get('data'):
                logger.warning(f"No insider sentiment data returned for {symbol} from {from_date} to {to_date}. Response: {sentiment_data}")
                return None
            
            logger.info(f"Successfully fetched insider sentiment for {symbol} from {from_date} to {to_date}. Number of monthly entries: {len(sentiment_data['data'])}")
            return sentiment_data # Contains {'data': [...], 'symbol': 'AAPL'}
        except finnhub.FinnhubAPIException as e:
            if e.status_code == 403:
                 logger.error(f"Finnhub API access forbidden (403) for insider sentiment on {symbol}: {e}.")
            elif e.status_code == 429: 
                 logger.error(f"Finnhub API rate limit hit (429) for insider sentiment on {symbol}: {e}")
            else:
                 logger.error(f"Finnhub API exception for insider sentiment on {symbol}: {e} (Status: {e.status_code})")
            return None
        except Exception as e:
            logger.error(f"Error fetching insider sentiment for {symbol} from Finnhub: {e}", exc_info=True)
            return None

    async def get_latest_market_news(self, category: str = 'general', min_id: int = 0) -> Optional[list[dict]]:
        """
        Fetches the latest market news from Finnhub.
        category: 'general', 'forex', 'crypto', 'merger'
        min_id: Only get news after this ID (default 0 = latest)
        Returns a list of news dicts or None on error.
        """
        try:
            loop = asyncio.get_running_loop()
            news = await loop.run_in_executor(None, self.client.general_news, category, min_id)
            logger.info(f"Fetched {len(news)} news items from Finnhub (category={category}).")
            return news
        except finnhub.FinnhubAPIException as e:
            logger.error(f"Finnhub API exception for market news: {e}")
            return None
        except Exception as e:
            logger.error(f"Error fetching market news from Finnhub: {e}", exc_info=True)
            return None


# Example Usage
async def main_finnhub_test():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s')
    
    fh_adapter = None
    try:
        # Initialize with update_sp500=True to ensure we have the latest S&P 500 list
        fh_adapter = FinnhubAdapter(update_sp500=True)
    except ValueError as e:
        logger.error(f"Failed to initialize FinnhubAdapter: {e}")
        return

    if not fh_adapter.sp500_symbols:
        logger.error("S&P 500 symbols could not be loaded from CSV. Aborting Finnhub tests.")
        return
    
    logger.info(f"Total S&P 500 symbols loaded from CSV for testing: {len(fh_adapter.sp500_symbols)}")
    if fh_adapter.sp500_symbols:
         logger.info(f"Sample of loaded S&P 500 symbols (first 5 from CSV): {fh_adapter.sp500_symbols[:5]}")

    await asyncio.sleep(1.2) # Small delay before hammering API

    # --- Fetch and print latest market news ---
    logger.info("\n--- Testing get_latest_market_news (category='general') ---")
    news = await fh_adapter.get_latest_market_news(category='general')
    if news:
        logger.info(f"Sample news headlines:")
        for item in news[:5]:
            logger.info(f"[{item.get('category')}] {item.get('headline')}\nSummary: {item.get('summary')}\nURL: {item.get('url')}\n")
    else:
        logger.warning("No news returned or error occurred.")

    # --- S&P 500 price fetching is commented out for now ---
    # logger.info("\n--- Testing get_latest_daily_quotes_for_sp500 (FULL LIST from CSV) ---")
    # if fh_adapter.sp500_symbols:
    #     start_time = time.time()
    #     sp500_data_full = await fh_adapter.get_latest_daily_quotes_for_sp500()
    #     end_time = time.time()
    #     successful_fetches = sum(1 for df in sp500_data_full.values() if df is not None)
    #     logger.info(f"Fetched quotes for {successful_fetches} out of {len(fh_adapter.sp500_symbols)} S&P 500 symbols.")
    #     logger.info(f"Total time for full S&P 500 quote fetch: {end_time - start_time:.2f} seconds.")
    #     count = 0
    #     for symbol, df in sp500_data_full.items():
    #         if df is not None and count < 3:
    #             logger.info(f"Full list - Sample Quote for {symbol}:\n{df}")
    #             count += 1
    #         elif df is None and count < 10: 
    #             logger.info(f"Full list - Failed to get quote for symbol: {symbol}")
    #             count +=1 
    # else:
    #     logger.warning("S&P 500 symbol list (from CSV) is empty. Skipping FULL S&P 500 quote test.")

if __name__ == '__main__':
    asyncio.run(main_finnhub_test()) 