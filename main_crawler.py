import asyncio
import logging
import argparse
import os
import sys

# Add the project root directory to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

# Import crawlers
from crawlers.cnstock_crawler import CnstockCrawler
from crawlers.jrj_crawler import JrjCrawler
from crawlers.nbd_crawler import NbdCrawler
from crawlers.sina_crawler import SinaCrawler
from crawlers.stcn_crawler import StcnCrawler
from crawlers.eastmoney_crawler import EastmoneyCrawler
from crawlers.eastmoney_market_crawler import EastmoneyMarketCrawler

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Map names to crawler classes
AVAILABLE_CRAWLERS = {
    "cnstock": CnstockCrawler,
    # "jrj": JrjCrawler, # Disabled due to persistent 514 errors
    "nbd": NbdCrawler,
    "sina": SinaCrawler,
    "stcn": StcnCrawler,
    "eastmoney": EastmoneyCrawler,
    "eastmoney_market": EastmoneyMarketCrawler,  # New market data crawler
}

async def run_crawler(crawler_class, limit):
    """Initializes, runs, and closes a single crawler."""
    crawler_name = crawler_class.__name__
    logger.info(f"Starting {crawler_name} with limit {limit}.")
    crawler_instance = None
    try:
        # You might want to pass mongo_uri from config here
        crawler_instance = crawler_class() 
        await crawler_instance.run(limit=limit)
        logger.info(f"{crawler_name} finished successfully.")
    except Exception as e:
        logger.error(f"Error running {crawler_name}: {e}", exc_info=True)
    finally:
        if crawler_instance:
            await crawler_instance.close()
            logger.info(f"{crawler_name} resources closed.")

async def main():
    parser = argparse.ArgumentParser(description="Run news crawlers.")
    parser.add_argument(
        "crawler_names",
        metavar="NAME",
        type=str,
        nargs="*", # Zero or more arguments
        help=f"Name(s) of the crawler(s) to run. Available: {', '.join(AVAILABLE_CRAWLERS.keys())}. If empty, runs all."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum number of articles to fetch per crawler (default: 10)."
    )
    # Add other arguments like --mongo-uri if needed

    args = parser.parse_args()

    crawlers_to_run = []
    if not args.crawler_names: # If no names provided, run all
        logger.info(f"No specific crawlers requested. Running all available crawlers: {', '.join(AVAILABLE_CRAWLERS.keys())}")
        crawlers_to_run = list(AVAILABLE_CRAWLERS.values())
    else:
        for name in args.crawler_names:
            crawler_class = AVAILABLE_CRAWLERS.get(name.lower())
            if crawler_class:
                crawlers_to_run.append(crawler_class)
            else:
                logger.warning(f"Crawler '{name}' not found. Skipping.")
    
    if not crawlers_to_run:
        logger.info("No valid crawlers selected to run. Exiting.")
        return

    tasks = [run_crawler(cls, args.limit) for cls in crawlers_to_run]
    await asyncio.gather(*tasks)

    logger.info("All requested crawler tasks have completed.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Crawler execution interrupted by user.")
    except Exception as e:
        logger.critical(f"Unhandled critical error in main_crawler: {e}", exc_info=True) 