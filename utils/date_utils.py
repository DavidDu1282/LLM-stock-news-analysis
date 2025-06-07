from datetime import datetime
from typing import Optional, List
import logging

# Configure logging
logger = logging.getLogger(__name__)

DEFAULT_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d %H:%M",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M",
    "%Y年%m月%d日 %H:%M",
    "%Y年%m月%d日%H时%M分", # Chinese format
    "%m月%d日 %H:%M",      # Chinese format without year (assumes current year)
    "%Y-%m-%d",
    "%Y/%m/%d",
    # Add more formats as needed
]

def parse_date_string_to_datetime(date_string: Optional[str], formats: Optional[List[str]] = None, silent: bool = False, relative_to_today_if_time_only: bool = False) -> Optional[datetime]:
    """
    Parses a date string into a datetime object using a list of possible formats.
    Handles common variations including Chinese date formats.
    If year is missing, assumes current year.
    If silent is True, suppresses warnings on parsing failure.
    If relative_to_today_if_time_only is True and the format parsed is time-only, it assumes the current date.
    """
    if not date_string:
        return None

    parse_formats = formats if formats else DEFAULT_FORMATS
    now = datetime.now()

    for fmt in parse_formats:
        try:
            dt = datetime.strptime(date_string, fmt)
            # If the format doesn't include year, and the original string doesn't seem to have it,
            # and the format is one that typically omits year (like "%m月%d日 %H:%M"),
            # assume current year.
            if "%Y" not in fmt and "%y" not in fmt:
                # A simple check: if the parsed year is default (1900), it means strptime used a default
                # because year wasn't in the format string. We should then set it to current year.
                if dt.year == 1900: # Default year for strptime when year is not in format
                    dt = dt.replace(year=now.year)
            
            # If relative_to_today_if_time_only is True, and we only parsed time components
            # (i.e., year, month, day are default from strptime or current year if year was missing in format),
            # set date to today.
            is_time_only_format = ("%Y" not in fmt and "%y" not in fmt and
                                   "%m" not in fmt and "%d" not in fmt) # Approximation of time-only format

            if relative_to_today_if_time_only and is_time_only_format:
                 # If dt still has year 1900, it means it was purely time.
                 # Or if year was set to current year but month/day were not in format (parsed as 1/1)
                if dt.year == 1900 or (dt.month == 1 and dt.day == 1 and ("%m" not in fmt and "%d" not in fmt)):
                    dt = dt.replace(year=now.year, month=now.month, day=now.day)
            
            return dt
        except ValueError:
            continue # Try the next format
    
    if not silent:
        logger.warning(f"Could not parse date string: '{date_string}' with any of the provided formats.")
    return None

# Example Usage (can be run directly for testing)
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_dates = [
        "2023-10-26 14:30:00",
        "2023/10/26 14:30",
        "10/26/2023 14:30:00",
        "2023年10月26日 14:30",
        "2023年10月26日14时30分",
        "10月26日 14:30", # Assumes current year
        "2023-10-26",
        "Some invalid date string",
        None
    ]

    for date_str in test_dates:
        parsed = parse_date_string_to_datetime(date_str)
        if parsed:
            logger.info(f"'{date_str}' -> {parsed} (Type: {type(parsed)})")
        else:
            logger.info(f"Failed to parse '{date_str}'")
    
    # Test with a specific format for a date that might be ambiguous or needs current year
    specific_date = "11-25 10:00" # Example: Nov 25, 10:00 AM (current year)
    custom_formats = ["%m-%d %H:%M"] # Format that omits year
    parsed_specific = parse_date_string_to_datetime(specific_date, formats=custom_formats)
    if parsed_specific:
        logger.info(f"'{specific_date}' (custom format) -> {parsed_specific} (Year: {parsed_specific.year})")
    else:
        logger.info(f"Failed to parse '{specific_date}' with custom format") 