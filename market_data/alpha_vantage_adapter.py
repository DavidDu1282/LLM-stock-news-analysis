import os
import logging
import asyncio
from alpha_vantage.async_support.timeseries import TimeSeries
from alpha_vantage.async_support.fundamentaldata import FundamentalData
# For news/sentiment, Alpha Vantage library might not have a direct async method yet,
# or it might be under a different module. We might need to make a direct HTTP request
# or check library updates. For now, let's assume we might use httpx for it.
import httpx 
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from config import settings

logger = logging.getLogger(__name__)

# Read API key from environment variable
ALPHA_VANTAGE_API_KEY = settings.ALPHA_VANTAGE_API_KEY
DEFAULT_SP500_CSV_PATH = "sp500_tickers.csv"

# DEFAULT_SYMBOLS_US = ['SPY', 'AAPL', 'MSFT', 'GOOGL'] # We'll replace this logic

def load_symbols_from_csv(csv_path: str = DEFAULT_SP500_CSV_PATH) -> List[str]:
    """Loads stock symbols from a CSV file. Expects a column named 'Symbol'."""
    try:
        df = pd.read_csv(csv_path)
        if "Symbol" not in df.columns:
            logger.error(f"'Symbol' column not found in {csv_path}")
            return []
        symbols = df["Symbol"].dropna().unique().tolist()
        logger.info(f"Loaded {len(symbols)} unique symbols from {csv_path}")
        return symbols
    except FileNotFoundError:
        logger.warning(f"Ticker CSV file not found at {csv_path}. Returning empty list.")
        return []
    except Exception as e:
        logger.error(f"Error loading symbols from {csv_path}: {e}", exc_info=True)
        return []

class AlphaVantageAdapter:
    def __init__(self, api_key: str = None, httpx_client: httpx.AsyncClient = None, sp500_csv_path: str = DEFAULT_SP500_CSV_PATH):
        self.api_key = api_key if api_key else ALPHA_VANTAGE_API_KEY
        if not self.api_key:
            logger.error("Alpha Vantage API key not provided. Please set the ALPHA_VANTAGE_API_KEY in config.settings or pass it during instantiation.")
            raise ValueError("Alpha Vantage API key is required.")
        
        self._client = httpx_client
        self.sp500_symbols = load_symbols_from_csv(sp500_csv_path)
        if not self.sp500_symbols:
            logger.warning("No S&P 500 symbols loaded. Time series functions for S&P 500 might not return data unless symbols are explicitly provided.")
        else:
            logger.info(f"AlphaVantageAdapter initialized with {len(self.sp500_symbols)} S&P 500 symbols.")

    async def _get_client(self) -> httpx.AsyncClient:
        """Returns the shared httpx client or creates a new one."""
        if self._client is None:
            # If you need specific configurations (proxies, timeouts), set them here
            self._client = httpx.AsyncClient() 
        return self._client

    async def close(self):
        """Closes the httpx client if it was created by this adapter."""
        if self._client:
            await self._client.aclose()
            self._client = None
            logger.info("AlphaVantageAdapter's httpx client closed.")

    async def get_daily_time_series(self, symbols: List[str] = None, output_size: str = 'compact', use_sp500_list: bool = False) -> Dict[str, Optional[pd.DataFrame]]:
        """
        Fetches daily time series data (OHLCV) for given symbols.
        If 'use_sp500_list' is True, it will use the loaded S&P 500 symbols list.
        Otherwise, it uses the 'symbols' argument, or a small default list if 'symbols' is None.
        output_size: 'compact' for last 100 days, 'full' for full history.
        Returns a dictionary where keys are symbols and values are DataFrames.
        """
        target_symbols = []
        if use_sp500_list:
            if not self.sp500_symbols:
                logger.warning("Attempted to use S&P 500 list, but it's empty or not loaded. Please check CSV path and contents.")
                return {}
            target_symbols = self.sp500_symbols
            logger.info(f"Fetching daily time series for {len(target_symbols)} symbols from S&P 500 list.")
        elif symbols is not None:
            target_symbols = symbols
        else:
            target_symbols = ['SPY', 'AAPL'] # Fallback default if no symbols and not using S&P500 list
            logger.info(f"No specific symbols provided and not using S&P 500 list. Using default symbols: {target_symbols}")
        
        if not target_symbols:
            logger.warning("No symbols to fetch data for.")
            return {}

        # Rate limit warning for large lists on free tier
        if len(target_symbols) > 5 and self.api_key and self.api_key.lower() == "demo": # A common free key from AV docs
            logger.warning("Fetching a large list of symbols with a DEMO key will likely hit rate limits quickly.")
        elif len(target_symbols) > 25: # General warning for free tier users
             logger.warning(f"Fetching data for {len(target_symbols)} symbols. This may take a long time and hit Alpha Vantage free tier rate limits (5 calls/min, 500 calls/day).")

        ts = TimeSeries(key=self.api_key, output_format='pandas', treat_info_as_error=True)
        results = {}
        call_count = 0
        
        for i, symbol in enumerate(target_symbols):
            try:
                logger.info(f"Fetching daily time series for {symbol} ({i+1}/{len(target_symbols)})... ")
                data, meta_data = await ts.get_daily(symbol=symbol, outputsize=output_size)
                data.columns = [col.split('. ')[1] if '. ' in col else col for col in data.columns]
                data.index = pd.to_datetime(data.index)
                results[symbol] = data
                logger.info(f"Successfully fetched daily time series for {symbol}. Shape: {data.shape}")
                call_count += 1
                
                # Respect rate limits: 5 calls per minute for free tier.
                # Stricter sleep if many symbols, less strict if few.
                if call_count % 4 == 0 and (len(target_symbols) - (i+1) > 0 ) : # After every 4 calls, if there are more symbols
                    logger.info("Approaching rate limit, pausing for 60 seconds...")
                    await asyncio.sleep(60) # Wait 60 seconds to reset the minute window for 5 calls.
                elif len(target_symbols) > 1 and (len(target_symbols) - (i+1) > 0 ):
                     await asyncio.sleep(1) # Small polite delay between individual calls if not hitting the batch limit

            except Exception as e:
                # Check for specific rate limit message from Alpha Vantage
                if "standard API call frequency is 5 calls per minute and 500 calls per day" in str(e).lower():
                    logger.error(f"Alpha Vantage Rate Limit Hit for {symbol}: {e}. Consider a paid plan or reducing frequency/number of symbols.")
                    # Potentially stop further calls or implement a longer backoff
                    results[symbol] = None # or re-raise to stop the process
                    break # Stop trying to fetch more symbols if rate limit is definitely hit.
                else:
                    logger.error(f"Error fetching daily time series for {symbol} from Alpha Vantage: {e}", exc_info=False) # Set exc_info to False to avoid huge logs for many errors
                    results[symbol] = None
            
        await ts.close()
        logger.info(f"Finished fetching time series for {len(results)} out of {len(target_symbols)} requested symbols.")
        return results

    async def get_news_sentiment(self, tickers: Optional[List[str]] = None, topics: Optional[List[str]] = None, limit: int = 50) -> Optional[List[Dict[str, Any]]]:
        """
        Fetches news and sentiment data.
        Alpha Vantage API endpoint: https://www.alphavantage.co/documentation/#news-sentiment
        'tickers': e.g., ['AAPL', 'MSFT']
        'topics': e.g., ['technology', 'earnings']
        'limit': Number of results to return, max 1000 for premium, typically less for free.
        
        Returns a list of news articles with sentiment.
        """
        base_url = "https://www.alphavantage.co/query"
        function = "NEWS_SENTIMENT"
        
        params = {
            "function": function,
            "apikey": self.api_key,
            "limit": str(limit) # API expects string for limit
        }
        
        if tickers:
            params["tickers"] = ",".join(tickers)
        if topics:
            # Topics could be: blockchain, earnings, ipo, mergers_and_acquisitions, 
            # financial_markets, economy_fiscal, economy_monetary, economy_macro,
            # energy_transportation, finance, life_sciences, manufacturing, 
            # real_estate, retail_wholesale, technology
            params["topics"] = ",".join(topics)

        try:
            client = await self._get_client()
            logger.info(f"Fetching news and sentiment from Alpha Vantage with params: {params}")
            response = await client.get(base_url, params=params)
            response.raise_for_status() # Raise an exception for bad status codes
            data = response.json()

            if "feed" in data:
                logger.info(f"Successfully fetched {len(data['feed'])} news/sentiment items.")
                return data["feed"]
            elif "Information" in data and "rate limit" in data["Information"]:
                logger.warning(f"Alpha Vantage rate limit hit for news/sentiment: {data['Information']}")
                return None
            elif "Error Message" in data:
                 logger.error(f"Alpha Vantage API error for news/sentiment: {data['Error Message']}")
                 return None
            else:
                logger.warning(f"Unexpected response structure for news/sentiment: {data}")
                return None
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error fetching news/sentiment from Alpha Vantage: {e.response.status_code} - {e.response.text}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Error fetching news/sentiment from Alpha Vantage: {e}", exc_info=True)
            return None

# Example Usage
async def main_alpha_vantage_test():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s')
    
    av_adapter = None
    try:
        # You can specify a different path to your S&P 500 CSV here if needed:
        # av_adapter = AlphaVantageAdapter(sp500_csv_path="path/to/your/sp500.csv")
        av_adapter = AlphaVantageAdapter()
    except ValueError as e:
        logger.error(f"Failed to initialize AlphaVantageAdapter: {e}")
        logger.info("Please set the ALPHA_VANTAGE_API_KEY in your config.settings.")
        return

    logger.info("--- Testing get_daily_time_series for a few default symbols ---")
    default_data = await av_adapter.get_daily_time_series(symbols=['GOOGL', 'TSLA'], output_size='compact')
    for symbol, data_df in default_data.items():
        if data_df is not None:
            logger.info(f"{symbol} daily data (last 2 days):\n{data_df.head(2)}")
    
    # Test with the S&P 500 list - THIS WILL BE SLOW AND LIKELY HIT RATE LIMITS ON FREE TIER
    # Only uncomment if you have a populated sp500_tickers.csv and understand the rate limits.
    # logger.info("\n--- Testing get_daily_time_series for S&P 500 list (first few symbols due to rate limits) ---")
    # if av_adapter.sp500_symbols: # Check if symbols were loaded
    #     # To test without hitting limits for too long, let's take a small slice
    #     test_sp500_symbols = av_adapter.sp500_symbols[:3] # Test with first 3 symbols from your CSV
    #     logger.info(f"Attempting to fetch for: {test_sp500_symbols}")
    #     sp500_sample_data = await av_adapter.get_daily_time_series(symbols=test_sp500_symbols, output_size='compact', use_sp500_list=False) # Explicitly pass symbols
        # Or to use the full loaded list (if you have a premium key or are prepared to wait/hit limits):
        # sp500_full_data = await av_adapter.get_daily_time_series(use_sp500_list=True, output_size='compact') 
    #     for symbol, data_df in sp500_sample_data.items():
    #         if data_df is not None:
    #             logger.info(f"{symbol} S&P 500 daily data (last 2 days):\n{data_df.head(2)}")
    # else:
    #     logger.warning("S&P 500 symbol list is empty. Skipping S&P 500 test.")

    # News sentiment test (keeping it short to avoid using too many API calls during testing)
    # await asyncio.sleep(60) # Wait a bit if you just ran a batch of time series calls
    logger.info("\n--- Testing get_news_sentiment (MSFT, finance topic) ---")
    news_items = await av_adapter.get_news_sentiment(tickers=['MSFT'], topics=['finance'], limit=3)
    if news_items:
        logger.info(f"Fetched {len(news_items)} news items for MSFT/finance.")
        for item_idx, item in enumerate(news_items):
            logger.info(f"  MSFT News Item {item_idx + 1}: Title: {item.get('title')}")
    else:
        logger.warning("No MSFT/finance news items fetched.")
    
    await av_adapter.close()

if __name__ == '__main__':
    asyncio.run(main_alpha_vantage_test()) 