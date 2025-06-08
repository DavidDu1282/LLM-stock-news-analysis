#!/bin/bash

# This script is intended to be run by cron.
# It sets up the environment and runs the Python news analyzer.

# --- Configuration ---
PROJECT_DIR="/home/ubuntu/LLM-stock-news-analysis"
PYTHON_EXEC="$PROJECT_DIR/.venv/bin/python"
ANALYZER_SCRIPT="$PROJECT_DIR/news_analyzer.py"
LOG_FILE="$PROJECT_DIR/cron.log"
# --- End Configuration ---

# Navigate to the project directory. Exit with an error if it fails.
cd "$PROJECT_DIR" || { echo "ERROR: Failed to navigate to $PROJECT_DIR. Exiting." >> "$LOG_FILE"; exit 1; }

# Log the start time for this run
echo "--- Cron job started at: $(date) ---" >> "$LOG_FILE"

# Execute the Python script.
# The analyzer's internal logic correctly selects the "morning" or "evening" pipeline based on the current time.
# We use --days 1 as a sensible default for a daily job.
# All output (both stdout and stderr) is appended to our log file.
"$PYTHON_EXEC" "$ANALYZER_SCRIPT" --days 1 >> "$LOG_FILE" 2>&1

# Check the exit code of the last command (the python script)
if [ $? -eq 0 ]; then
  echo "Python script finished successfully." >> "$LOG_FILE"
else
  # Log an error if the script returned a non-zero exit code
  echo "ERROR: Python script exited with a failure code." >> "$LOG_FILE"
fi

# Log the end time and add a blank line for readability in the log
echo "--- Cron job finished at: $(date) ---" >> "$LOG_FILE"
echo "" >> "$LOG_FILE" 