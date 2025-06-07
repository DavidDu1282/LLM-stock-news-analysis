import argparse
import logging
from datetime import datetime, timedelta
from pymongo import MongoClient

# --- Configuration ---
MONGO_URI = "mongodb://localhost:27017/"
# Add all database and collection names that should be cleaned up.
# Format: ("database_name", "collection_name")
COLLECTIONS_TO_CLEAN = [
    ("Finnhub_News", "market_news"),
    ("Sina_Stock", "sina_news_company"),
    ("Jrj_Stock", "jrj_news_company"),
    ("Cnstock_Stock", "cnstock_news_company"),
    ("Stcn_Stock", "stcn_news_company"),
    ("eastmoney", "eastmoney_news_company"),
    ("dragon_tiger_list", "eastmoney_market_data") # Assuming this is the new name
]

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def clean_old_articles(mongo_uri: str, retention_days: int, dry_run: bool = True):
    """
    Connects to MongoDB and deletes articles older than the specified retention period.

    Args:
        mongo_uri (str): The connection string for MongoDB.
        retention_days (int): The number of days to keep articles. Articles older
                              than this will be removed.
        dry_run (bool): If True, only prints the number of documents that would be
                        deleted without actually performing the deletion.
    """
    if dry_run:
        logger.info("--- Starting database maintenance in DRY RUN mode. No data will be deleted. ---")
    else:
        logger.warning("--- Starting database maintenance in ACTIVE mode. Data WILL be deleted. ---")

    try:
        client = MongoClient(mongo_uri)
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        total_deleted_count = 0

        logger.info(f"Purging all articles older than {retention_days} days (before {cutoff_date.strftime('%Y-%m-%d')}).")

        for db_name, collection_name in COLLECTIONS_TO_CLEAN:
            try:
                db = client[db_name]
                collection = db[collection_name]
                
                # The query to find old documents based on the 'date' field
                # If your date field is named differently, change "date" here.
                query = {"date": {"$lt": cutoff_date}}

                # First, count the documents to be deleted
                count_to_delete = collection.count_documents(query)
                
                if count_to_delete == 0:
                    logger.info(f"[{db_name}/{collection_name}]: No old articles found to delete.")
                    continue

                if dry_run:
                    logger.info(f"[{db_name}/{collection_name}]: Would delete {count_to_delete} articles.")
                else:
                    logger.info(f"[{db_name}/{collection_name}]: Deleting {count_to_delete} old articles...")
                    result = collection.delete_many(query)
                    # Verify the deletion was successful
                    if result.deleted_count == count_to_delete:
                        logger.info(f"[{db_name}/{collection_name}]: Successfully deleted {result.deleted_count} articles.")
                    else:
                        logger.warning(f"[{db_name}/{collection_name}]: Deletion mismatch. Expected to delete {count_to_delete}, but deleted {result.deleted_count}.")

                total_deleted_count += count_to_delete

            except Exception as e:
                logger.error(f"Could not process collection {db_name}/{collection_name}. Error: {e}", exc_info=True)
        
        if dry_run:
            logger.info(f"--- DRY RUN COMPLETE. Total articles that would be deleted: {total_deleted_count} ---")
        else:
            logger.info(f"--- Maintenance complete. Total articles deleted: {total_deleted_count} ---")

    except Exception as e:
        logger.critical(f"A critical error occurred during database maintenance: {e}", exc_info=True)
    finally:
        if 'client' in locals() and client:
            client.close()
            logger.info("MongoDB connection closed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Database maintenance script to clean up old news articles.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Retention period in days. Articles older than this will be deleted.\n(Default: 30)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform a dry run without deleting any data.\n(Highly recommended for the first run)"
    )
    args = parser.parse_args()

    clean_old_articles(
        mongo_uri=MONGO_URI,
        retention_days=args.days,
        dry_run=args.dry_run
    ) 