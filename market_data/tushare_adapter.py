import tushare as ts
import pandas as pd
import logging
import os
from datetime import datetime, timedelta
from typing import Optional, Dict
from config import settings

logger = logging.getLogger(__name__)

# Read token from environment variable
TUSHARE_API_TOKEN = settings.TUSHARE_API_TOKEN

class TushareAdapter:
    def __init__(self, token: str = None):
        actual_token = token if token else TUSHARE_API_TOKEN
        if not actual_token:
            logger.error("Tushare API token not provided. Please set the TUSHARE_API_TOKEN environment variable or pass it during instantiation.")
            raise ValueError("Tushare API token is required.")
        
        # Check for placeholder value if it somehow gets through environment variable being literally set to this string
        if actual_token == "YOUR_TUSHARE_API_TOKEN_HERE":
             logger.warning("Using a placeholder Tushare API token string. Please ensure TUSHARE_API_TOKEN environment variable is set correctly.")
             # Depending on policy, you might still want to raise ValueError here

        self.pro = ts.pro_api(actual_token)
        logger.info("Tushare Pro API initialized.")

    def get_stock_list(self) -> Optional[pd.DataFrame]:
        """Fetches the list of all stocks (stock_basic)."""
        try:
            data = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
            logger.info(f"Successfully fetched {len(data)} stocks from Tushare.")
            return data
        except Exception as e:
            logger.error(f"Error fetching stock list from Tushare: {e}", exc_info=True)
            return None

    def get_daily_market_data(self, trade_date: str = None, ts_code: str = None) -> Optional[pd.DataFrame]:
        """
        Fetches daily market data (OHLCV, percent change, etc.) for specific stocks or all stocks for a trade_date.
        trade_date format: YYYYMMDD
        ts_code: Tushare stock code, e.g., '600519.SH'. If None, fetches for all stocks (might be large).
        """
        if not trade_date:
            trade_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d') # Default to yesterday
            logger.info(f"trade_date not provided, defaulting to {trade_date}")

        try:
            if ts_code:
                df = self.pro.daily(ts_code=ts_code, trade_date=trade_date)
            else:
                # Fetching for all stocks on a specific date
                df = self.pro.daily(trade_date=trade_date)
            
            logger.info(f"Fetched daily market data for {ts_code or 'all stocks'} on {trade_date}. Rows: {len(df) if df is not None else 0}")
            return df
        except Exception as e:
            logger.error(f"Error fetching daily market data from Tushare for {ts_code or 'all stocks'} on {trade_date}: {e}", exc_info=True)
            return None

    def get_daily_basic_metrics(self, trade_date: str = None, ts_code: str = None) -> Optional[pd.DataFrame]:
        """
        Fetches daily basic metrics (PE, PB, turnover rate, total market cap, etc.).
        trade_date format: YYYYMMDD
        ts_code: Tushare stock code. If None, fetches for all stocks.
        """
        if not trade_date:
            trade_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
            logger.info(f"trade_date not provided for daily_basic, defaulting to {trade_date}")
        
        try:
            if ts_code:
                df = self.pro.daily_basic(ts_code=ts_code, trade_date=trade_date)
            else:
                df = self.pro.daily_basic(trade_date=trade_date)
            logger.info(f"Fetched daily basic metrics for {ts_code or 'all stocks'} on {trade_date}. Rows: {len(df) if df is not None else 0}")
            return df
        except Exception as e:
            logger.error(f"Error fetching daily basic metrics from Tushare for {ts_code or 'all stocks'} on {trade_date}: {e}", exc_info=True)
            return None

    def get_major_movers(self, trade_date: str = None, top_n: int = 10) -> Dict[str, pd.DataFrame]:
        """
        Identifies top N gainers and losers for a given trade date.
        Returns a dictionary with 'gainers' and 'losers' DataFrames.
        """
        daily_data = self.get_daily_market_data(trade_date=trade_date)
        if daily_data is None or daily_data.empty:
            logger.warning(f"No daily market data to identify major movers for {trade_date}.")
            return {"gainers": pd.DataFrame(), "losers": pd.DataFrame()}

        # Ensure 'pct_chg' column exists and is numeric
        if 'pct_chg' not in daily_data.columns:
            logger.error("'pct_chg' column not found in daily market data.")
            return {"gainers": pd.DataFrame(), "losers": pd.DataFrame()}
        
        try:
            daily_data['pct_chg'] = pd.to_numeric(daily_data['pct_chg'], errors='coerce')
            daily_data = daily_data.dropna(subset=['pct_chg']) # Remove rows where conversion failed
        except Exception as e:
            logger.error(f"Error converting 'pct_chg' to numeric: {e}")
            return {"gainers": pd.DataFrame(), "losers": pd.DataFrame()}


        gainers = daily_data.sort_values(by='pct_chg', ascending=False).head(top_n)
        losers = daily_data.sort_values(by='pct_chg', ascending=True).head(top_n)
        
        logger.info(f"Identified top {top_n} gainers and losers for {trade_date}.")
        return {"gainers": gainers, "losers": losers}

    def get_sector_performance(self, trade_date: str = None) -> Optional[pd.DataFrame]:
        """
        Fetches daily performance data for all sectors/industries.
        Returns a DataFrame with sector performance metrics.
        """
        if not trade_date:
            trade_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
            logger.info(f"trade_date not provided for sector performance, defaulting to {trade_date}")

        try:
            # Get all industry indices
            indices = self.pro.index_classify(level='L1', src='SW')
            if indices is None or indices.empty:
                logger.error("Failed to fetch industry indices")
                return None

            # Get daily data for each industry index
            sector_data = []
            for _, row in indices.iterrows():
                try:
                    daily = self.pro.index_daily(ts_code=row['ts_code'], trade_date=trade_date)
                    if daily is not None and not daily.empty:
                        daily['industry_name'] = row['industry_name']
                        sector_data.append(daily)
                except Exception as e:
                    logger.warning(f"Error fetching data for sector {row['industry_name']}: {e}")
                    continue

            if not sector_data:
                logger.warning(f"No sector data found for {trade_date}")
                return None

            # Combine all sector data
            result = pd.concat(sector_data, ignore_index=True)
            # Sort by percentage change
            result = result.sort_values(by='pct_chg', ascending=False)
            
            logger.info(f"Successfully fetched sector performance data for {trade_date}")
            return result
        except Exception as e:
            logger.error(f"Error fetching sector performance data: {e}", exc_info=True)
            return None

    def get_sector_constituents(self, sector_code: str) -> Optional[pd.DataFrame]:
        """
        Fetches the list of stocks that belong to a specific sector/industry.
        sector_code: The Tushare industry code (e.g., from index_classify)
        """
        try:
            constituents = self.pro.index_member(index_code=sector_code)
            if constituents is None or constituents.empty:
                logger.warning(f"No constituents found for sector {sector_code}")
                return None
            
            logger.info(f"Successfully fetched {len(constituents)} constituents for sector {sector_code}")
            return constituents
        except Exception as e:
            logger.error(f"Error fetching sector constituents for {sector_code}: {e}", exc_info=True)
            return None

# Example Usage (for testing this module directly)
async def main_tushare_test():
    import asyncio
    logging.basicConfig(level=logging.INFO)
    
    # IMPORTANT: Ensure TUSHARE_API_TOKEN environment variable is set before running.
    try:
        adapter = TushareAdapter()
    except ValueError as e:
        logger.error(f"Failed to initialize TushareAdapter: {e}")
        logger.info("Please set the TUSHARE_API_TOKEN environment variable.")
        logger.info("Example: export TUSHARE_API_TOKEN='your_actual_token_here' (Linux/macOS)")
        logger.info("Example: $env:TUSHARE_API_TOKEN='your_actual_token_here' (PowerShell)")
        logger.info("Example: set TUSHARE_API_TOKEN=your_actual_token_here (Windows CMD)")
        return

    logger.info("--- Testing get_stock_list ---")
    stock_list = adapter.get_stock_list()
    if stock_list is not None:
        logger.info(f"First 5 stocks:\n{stock_list.head()}")

    # Test with a specific recent trade date (YYYYMMDD format)
    # Adjust this date to a valid recent trading day if needed
    test_trade_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d') 
    # You might need to adjust if yesterday was not a trading day. 
    # Tushare also has a trade_cal endpoint to check trading days.
    
    logger.info(f"--- Testing get_daily_market_data for a specific stock on {test_trade_date} ---")
    # Example stock: Ping An Bank (平安银行)
    daily_data_single = adapter.get_daily_market_data(ts_code='000001.SZ', trade_date=test_trade_date)
    if daily_data_single is not None:
        logger.info(f"Daily data for 000001.SZ:\n{daily_data_single}")

    logger.info(f"--- Testing get_daily_basic_metrics for a specific stock on {test_trade_date} ---")
    daily_basic_single = adapter.get_daily_basic_metrics(ts_code='000001.SZ', trade_date=test_trade_date)
    if daily_basic_single is not None:
        logger.info(f"Daily basic metrics for 000001.SZ:\n{daily_basic_single}")

    logger.info(f"--- Testing get_major_movers for {test_trade_date} ---")
    movers = adapter.get_major_movers(trade_date=test_trade_date, top_n=5)
    if movers["gainers"] is not None:
        logger.info(f"Top 5 Gainers:\n{movers['gainers']}")
    if movers["losers"] is not None:
        logger.info(f"Top 5 Losers:\n{movers['losers']}")
    
    # Example fetching all daily data for a date (can be large, use with caution or ensure date is specific)
    # logger.info(f"--- Testing get_daily_market_data for ALL stocks on {test_trade_date} ---")
    # all_daily_data = adapter.get_daily_market_data(trade_date=test_trade_date)
    # if all_daily_data is not None:
    #     logger.info(f"All daily data for {test_trade_date} (first 5 rows):\n{all_daily_data.head()}")


if __name__ == '__main__':
    import asyncio
    # Note: Tushare SDK is synchronous, so direct asyncio.run is fine for this example.
    # If integrating into a larger async app, you might run these synchronous calls 
    # in a thread pool executor (e.g., asyncio.to_thread in Python 3.9+)
    asyncio.run(main_tushare_test()) 