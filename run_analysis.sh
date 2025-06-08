#!/bin/bash

# This script is intended to be run by cron.
# It runs the full news pipeline: crawling and then analyzing.

# --- Configuration ---
PROJECT_DIR="/home/ubuntu/LLM-stock-news-analysis"
PYTHON_EXEC="$PROJECT_DIR/.venv/bin/python"
ANALYZER_SCRIPT="$PROJECT_DIR/news_analyzer.py"
CRAWLER_SCRIPT="$PROJECT_DIR/main_crawler.py"
LOG_FILE="$PROJECT_DIR/cron.log"
ARTICLES_TO_FETCH=50 # Number of articles to crawl and analyze per run
# --- End Configuration ---

# Navigate to the project directory. Exit with an error if it fails.
cd "$PROJECT_DIR" || { echo "ERROR: Failed to navigate to $PROJECT_DIR. Exiting." >> "$LOG_FILE"; exit 1; }

# Log the start time for this run
echo "======================================================================" >> "$LOG_FILE"
echo "PIPELINE STARTED AT: $(date)" >> "$LOG_FILE"
echo "======================================================================" >> "$LOG_FILE"

# --- Step 1: Run Crawlers ---
echo "[$(date)] Starting crawlers (fetching up to $ARTICLES_TO_FETCH articles per source)..." >> "$LOG_FILE"
"$PYTHON_EXEC" "$CRAWLER_SCRIPT" --limit "$ARTICLES_TO_FETCH" >> "$LOG_FILE" 2>&1
if [ $? -eq 0 ]; then
  echo "[$(date)] Crawlers finished successfully." >> "$LOG_FILE"
else
  echo "[$(date)] ERROR: Crawlers exited with a failure code." >> "$LOG_FILE"
fi
echo "---" >> "$LOG_FILE"

# --- Step 2: Run Analyzer ---
echo "[$(date)] Starting analyzer (processing up to $ARTICLES_TO_FETCH articles per source)..." >> "$LOG_FILE"
"$PYTHON_EXEC" "$ANALYZER_SCRIPT" --days 1 --limit "$ARTICLES_TO_FETCH" >> "$LOG_FILE" 2>&1
if [ $? -eq 0 ]; then
  echo "[$(date)] Analyzer finished successfully." >> "$LOG_FILE"
else
  echo "[$(date)] ERROR: Analyzer exited with a failure code." >> "$LOG_FILE"
fi

# Log the end time and add a blank line for readability in the log
echo "======================================================================" >> "$LOG_FILE"
echo "PIPELINE FINISHED AT: $(date)" >> "$LOG_FILE"
echo "======================================================================" >> "$LOG_FILE"
echo "" >> "$LOG_FILE" 