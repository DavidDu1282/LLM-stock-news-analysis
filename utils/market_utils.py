import requests
import pandas as pd
import logging
import os
import sys

# Add project root to sys.path to allow for correct module imports
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, PROJECT_ROOT)

logger = logging.getLogger(__name__)

# Define the default path for the S&P 500 tickers CSV in the project root
DEFAULT_SP500_CSV_PATH_FOR_UTILS = os.path.join(PROJECT_ROOT, "sp500_tickers.csv")

def get_sp500_tickers_wikipedia(timeout: int = 10) -> pd.DataFrame:
    """
    Fetches the list of S&P 500 ticker symbols, company names, and sectors from Wikipedia.

    Returns:
        pd.DataFrame: A DataFrame containing 'Symbol', 'Security' (Company Name), and 'GICS Sector' columns.
                     Returns an empty DataFrame if fetching fails.
    """
    wiki_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        logger.info(f"Fetching S&P 500 constituents from Wikipedia: {wiki_url}")
        response = requests.get(wiki_url, headers=headers, timeout=timeout)
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        # pandas.read_html returns a list of DataFrames
        # The S&P 500 constituents table is usually the first one.
        tables = pd.read_html(response.content, flavor='bs4') # Use BeautifulSoup4 parser
        
        if not tables:
            logger.warning("No tables found on the Wikipedia page.")
            return pd.DataFrame(columns=['Symbol', 'Security', 'GICS Sector'])

        # Heuristic to find the S&P 500 table:
        # It typically has "Symbol", "Security", "GICS Sector", "GICS Sub-Industry"
        sp500_table = None
        for table in tables:
            if "Symbol" in table.columns and "Security" in table.columns and "GICS Sector" in table.columns:
                sp500_table = table
                logger.info("Found S&P 500 constituents table on Wikipedia.")
                break
        
        if sp500_table is None:
            logger.warning("Could not identify the S&P 500 constituents table on the Wikipedia page.")
            return pd.DataFrame(columns=['Symbol', 'Security', 'GICS Sector'])
            
        # Ensure required columns exist
        required_columns = ['Symbol', 'Security', 'GICS Sector']
        if not all(col in sp500_table.columns for col in required_columns):
            logger.warning(f"Required columns {required_columns} not all found in the identified table.")
            return pd.DataFrame(columns=required_columns)
            
        # Select only the required columns and drop any rows with missing values
        sp500_table = sp500_table[required_columns].dropna()
        
        # Clean symbols: Some symbols on Wikipedia might have dots (e.g., BRK.B) 
        # which financial APIs often expect with dashes (BRK-B).
        sp500_table['Symbol'] = sp500_table['Symbol'].str.replace('.', '-')
        
        logger.info(f"Successfully fetched and parsed {len(sp500_table)} S&P 500 companies from Wikipedia.")
        return sp500_table
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching Wikipedia page: {e}", exc_info=True)
        return pd.DataFrame(columns=['Symbol', 'Security', 'GICS Sector'])
    except Exception as e:
        logger.error(f"Error parsing S&P 500 tickers from Wikipedia: {e}", exc_info=True)
        return pd.DataFrame(columns=['Symbol', 'Security', 'GICS Sector'])

def update_sp500_csv(csv_path: str = DEFAULT_SP500_CSV_PATH_FOR_UTILS) -> bool:
    """
    Fetches S&P 500 tickers, company names, and sectors from Wikipedia and updates the specified CSV file.
    The CSV will have columns: "Symbol", "Name", "Sector".
    """
    logger.info(f"Attempting to update S&P 500 tickers CSV: {csv_path}")
    sp500_df = get_sp500_tickers_wikipedia()
    
    if sp500_df.empty:
        logger.error("Failed to fetch data from Wikipedia. CSV will not be updated.")
        return False
        
    try:
        # Rename columns to match the desired CSV format
        sp500_df = sp500_df.rename(columns={
            'Security': 'Name',
            'GICS Sector': 'Sector'
        })
        
        # Save to CSV
        sp500_df.to_csv(csv_path, index=False)
        logger.info(f"Successfully updated S&P 500 tickers CSV at {csv_path} with {len(sp500_df)} companies.")
        logger.info(f"The updated CSV now contains 'Symbol', 'Name', and 'Sector' columns.")
        return True
    except Exception as e:
        logger.error(f"Error writing S&P 500 data to CSV {csv_path}: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Option 1: Just get the DataFrame
    # sp500_df = get_sp500_tickers_wikipedia()
    # if not sp500_df.empty:
    #     logger.info(f"Fetched {len(sp500_df)} S&P 500 companies. First 5 rows:\n{sp500_df.head()}")
    # else:
    #     logger.info("Could not fetch S&P 500 data from Wikipedia.")

    # Option 2: Get the data and update/create sp500_tickers.csv in the project root
    logger.info(f"Running market_utils.py to update S&P 500 tickers CSV.")
    success = update_sp500_csv()
    if success:
        logger.info("sp500_tickers.csv update process completed successfully.")
    else:
        logger.error("sp500_tickers.csv update process failed.")

    # To use this in your FinnhubAdapter or AlphaVantageAdapter:
    # 1. Import: from utils.market_utils import get_sp500_tickers_wikipedia
    # 2. Call: sp500_df = get_sp500_tickers_wikipedia()
    # 3. Get symbols: symbols = sp500_df['Symbol'].tolist()
    # 4. Fallback: if symbols is empty: symbols = load_symbols_from_csv_fallback(...) 