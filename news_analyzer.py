# Standard Library Imports
import asyncio
import logging
import os
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import textwrap
import argparse

# Third-Party Imports
import pandas as pd
from bson import ObjectId
from pymongo import MongoClient, UpdateOne

# Local Application Imports
from config import settings
from email_utils import EmailService
from llm_utils import (GEMINI_MODELS_CONFIG, create_clients,
                       send_query_to_first_available_model)
from market_data.tushare_adapter import TushareAdapter
from crawlers.eastmoney_market_crawler import EastmoneyMarketCrawler


# --- Configure Logging ---
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def extract_analysis_details(analysis_text: str) -> Dict:
    """
    Extracts structured information from the LLM's analysis text using a simple, robust format.
    """
    if not analysis_text:
        logger.warning("Analysis text is empty. Returning default details.")
        return {
            "importance_score": 0,
            "sectors": [],
            "sentiment_score": 0,
            "analysis_summary": ""
        }
    
    details = {
        "importance_score": 0,
        "sectors": [],
        "sentiment_score": 0,
        "analysis_summary": ""
    }

    try:
        # Importance Score
        importance_match = re.search(r"Importance_Score:\s*(\d+)", analysis_text, re.IGNORECASE)
        if importance_match:
            details["importance_score"] = int(importance_match.group(1))
            logger.debug(f"Found importance score: {details['importance_score']}")
        else:
            logger.debug("Importance_Score pattern not found.")

        # Sentiment Score
        sentiment_match = re.search(r"Sentiment_Score:\s*(\d+)", analysis_text, re.IGNORECASE)
        if sentiment_match:
            details["sentiment_score"] = int(sentiment_match.group(1))
            logger.debug(f"Found sentiment score: {details['sentiment_score']}")
        else:
            logger.debug("Sentiment_Score pattern not found.")

        # Affected Sectors
        sector_section_match = re.search(r"Affected_Sectors_Start(.*?)Affected_Sectors_End", analysis_text, re.DOTALL | re.IGNORECASE)
        if sector_section_match:
            sector_text = sector_section_match.group(1)
            # Find all list items (lines starting with -)
            sector_lines = re.findall(r"^\s*-\s*(.*)", sector_text, re.MULTILINE)
            if sector_lines:
                details["sectors"] = [line.strip() for line in sector_lines]
                logger.debug(f"Found sectors: {details['sectors']}")
            else:
                logger.debug("Sector items format ('- [Sector]: [Description]') not found in sector section.")
        else:
            logger.debug("Affected_Sectors_Start/End block not found.")
        
        # Analysis Summary
        summary_match = re.search(r"Analysis_Summary:\s*(.*)", analysis_text, re.DOTALL | re.IGNORECASE)
        if summary_match:
            details["analysis_summary"] = summary_match.group(1).strip()
            logger.debug(f"Found analysis summary.")
        else:
            logger.debug("Analysis_Summary pattern not found.")

        logger.debug(f"Finished extraction. Details: Importance={details['importance_score']}, Sentiment={details['sentiment_score']}")
    except Exception as e:
        logger.error(f"Error during regex extraction in extract_analysis_details: {e}", exc_info=True)
    
    return details


class NewsAnalyzer:
    def __init__(self, mongo_uri: str = "mongodb://localhost:27017/"):
        self.client = MongoClient(mongo_uri)
        logger.info(f"NewsAnalyzer initialized with MongoDB URI: {mongo_uri}")
        try:
            self.studio_client, self.vertex_client = create_clients()
            logger.info("LLM clients created successfully.")
            
            self.tushare_adapter = TushareAdapter()
            logger.info("Tushare adapter initialized successfully.")
            
            self.eastmoney_crawler = EastmoneyMarketCrawler()
            logger.info("Eastmoney crawler initialized successfully.")
            
            self.email_service = EmailService()
            logger.info("Email service initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize components: {e}", exc_info=True)
            self.studio_client, self.vertex_client = None, None
            self.tushare_adapter = None
            self.eastmoney_crawler = None
            self.email_service = None

    async def get_market_movers(self):
        """
        Fetches and standardizes top market movers (gainers and losers) for the day
        from Eastmoney (Dragon and Tiger List) and Tushare (Sector Performance).
        """
        all_movers = []
        
        # 1. Fetch from Eastmoney Dragon and Tiger List
        try:
            # Note: This returns a pandas DataFrame
            lhb_df = await self.eastmoney_crawler.fetch_dragon_tiger_list()
            if lhb_df is not None and not lhb_df.empty:
                logger.info(f"Successfully fetched {len(lhb_df)} records from Eastmoney LHB.")
                for index, item in lhb_df.iterrows():
                    try:
                        all_movers.append({
                            "name": item.get("SECURITY_NAME_ABBR"),
                            "change_pct": float(item.get("CHANGE_RATE", 0)),
                            "reason": item.get("EXPLAIN")
                        })
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Could not parse item from LHB data: {item}. Error: {e}")
        except Exception as e:
            logger.error(f"Error fetching data from Eastmoney LHB: {e}", exc_info=True)

        # 2. Fetch from Tushare Sector Performance
        try:
            sector_data = self.tushare_adapter.get_sector_performance()
            if sector_data is not None and not sector_data.empty:
                logger.info(f"Successfully fetched {len(sector_data)} records from Tushare Sectors.")
                for index, item in sector_data.iterrows():
                    try:
                        all_movers.append({
                            "name": item.get("name"),
                            "change_pct": float(item.get("pct_chg", 0)),
                            "reason": "Sector Performance"
                        })
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Could not parse item from Tushare sector data: {item}. Error: {e}")
        except Exception as e:
            logger.error(f"Error fetching data from Tushare Sectors: {e}", exc_info=True)

        if not all_movers:
            logger.warning("No market mover data available from any source.")
            return {"gainers": [], "losers": []}

        # 3. Sort and return top gainers and losers
        seen_names = set()
        unique_movers = []
        for mover in all_movers:
            if mover.get('name') and mover['name'] not in seen_names:
                unique_movers.append(mover)
                seen_names.add(mover['name'])

        sorted_movers = sorted(unique_movers, key=lambda x: x.get('change_pct', 0), reverse=True)
        
        gainers = [m for m in sorted_movers if m.get('change_pct', 0) > 0][:10]
        losers = sorted([m for m in sorted_movers if m.get('change_pct', 0) < 0], key=lambda x: x.get('change_pct', 0))[:10]

        logger.info(f"Identified {len(gainers)} top gainers and {len(losers)} top losers.")
        return {"gainers": gainers, "losers": losers}

    def format_market_movers_for_prompt(self, market_movers: Dict) -> str:
        """Formats the market movers data into a string for the LLM prompt."""
        
        def format_movers_list(movers: List[Dict], category_name: str) -> str:
            if not movers:
                return f"{category_name}: None"
            
            formatted_list = []
            for m in movers:
                name = m.get('name', 'N/A')
                change = m.get('change_pct', 0.0)
                formatted_list.append(f"{name}: {change:.2f}%")
            return f"{category_name}: {', '.join(formatted_list)}"

        gainers_str = format_movers_list(market_movers.get("gainers", []), "Top Gainers")
        losers_str = format_movers_list(market_movers.get("losers", []), "Top Losers")

        return f"Market Context:\n{gainers_str}\n{losers_str}"

    def get_us_china_stock_mapping(self) -> Dict[str, List[str]]:
        """
        Returns a mapping of US stocks to their Chinese counterparts.
        This is a simplified version - you might want to expand this with more sophisticated mapping logic.
        """
        return {
            "TSLA": ["002594.SZ", "300750.SZ"],  # Tesla -> BYD, CATL
            "AAPL": ["002475.SZ", "002241.SZ"],  # Apple -> Luxshare, Goertek
            "NVDA": ["002049.SZ", "300782.SZ"],  # NVIDIA -> Unisoc, Cambricon
            "AMD": ["688012.SH", "688396.SH"],   # AMD -> Montage, Hygon
            "INTC": ["688012.SH", "688396.SH"],  # Intel -> Montage, Hygon
            "MSFT": ["002230.SZ", "300454.SZ"],  # Microsoft -> Kingsoft, Wondershare
            "GOOGL": ["002230.SZ", "300454.SZ"], # Google -> Kingsoft, Wondershare
            "META": ["002230.SZ", "300454.SZ"],  # Meta -> Kingsoft, Wondershare
            "AMZN": ["002024.SZ", "002251.SZ"],  # Amazon -> Suning, 360
            "NFLX": ["300413.SZ", "300133.SZ"],  # Netflix -> iQiyi, Huace
            # Add more mappings as needed
        }

    async def analyze_news_article(self, article: Dict, analysis_type: str = "evening") -> Dict:
        """
        Analyze a single news article using Gemini LLM.
        analysis_type: "morning" or "evening" to determine the focus of analysis
        """
        article_id = article.get("_id", "N/A")
        article_title = article.get("title", "N/A")
        logger.info(f"Starting {analysis_type} analysis for article ID: {article_id}, Title: '{article_title[:50]}...'")

        # Get market context
        market_context = ""
        
        prompt = "" # Initialize prompt to prevent UnboundLocalError
        if analysis_type == "evening":
            movers = await self.get_market_movers()
            market_context = self.format_market_movers_for_prompt(movers)
            prompt = f"""
            Analyze the following Chinese news article in the context of today's market performance.

            **Market Context:**
            {market_context}

            **News Article:**
            - **Title:** "{article.get('title', 'N/A')}"
            - **Content:** "{article.get('content', 'N/A')}"
            - **Source:** "{article.get('source', 'N/A')}"
            - **URL:** {article.get('url', 'N/A')}

            **Your Task:**
            Provide a structured analysis in Chinese. Your entire response MUST strictly follow the format below, using the exact headers without any markdown (like ** or *):

            Importance_Score: [1-10, where 10 is critically important]
            Sentiment_Score: [1-10, where 1 is very negative, 5 is neutral, 10 is very positive]
            Affected_Sectors_Start
            - [Sector Name 1]: [Brief explanation of the impact]
            - [Sector Name 2]: [Brief explanation of the impact]
            Affected_Sectors_End
            Analysis_Summary: [A concise summary of your reasoning and predictive analysis, explaining why this news is or isn't important based on the market context.]
            """
        else:  # morning analysis
            prompt = f"""
            Analyze the following US news article and its potential impact on the Chinese stock market.

            **News Article:**
            - **Title:** "{article.get('title', 'N/A')}"
            - **Content:** "{article.get('content', 'N/A')}"
            - **Source:** "{article.get('source', 'N/A')}"
            - **URL:** {article.get('url', 'N/A')}

            **Your Task:**
            Provide a structured analysis in Chinese. Your entire response MUST strictly follow the format below, using the exact headers without any markdown (like ** or *):

            Importance_Score: [1-10]
            Sentiment_Score: [1-10]
            Affected_Sectors_Start
            - [Sector Name 1]: [Brief explanation of the impact]
            - [Sector Name 2]: [Brief explanation of the impact]
            Affected_Sectors_End
            Analysis_Summary: [A concise summary of your reasoning and predictive analysis]

            **Example Response Format:**
            Importance_Score: 7
            Sentiment_Score: 8
            Affected_Sectors_Start
            - 新能源汽车: 美国市场的政策变化可能影响中国相关产业链的出口预期。
            - 半导体: 对全球供应链的担忧可能传导至中国的芯片设计和制造公司。
            Affected_Sectors_End
            Analysis_Summary: 该新闻预示着美国新能源政策的重大转变，可能对中国的相关出口构成挑战，但也会加速国内市场的整合。预计短期内相关板块将承压。
            """

        if not prompt:
            logger.error(f"Prompt was not generated for article {article_id} with analysis type {analysis_type}.")
            return {}

        try:
            analysis_text = ""
            if self.studio_client or self.vertex_client:
                llm_output = send_query_to_first_available_model(
                    query=prompt,
                    studio_client=self.studio_client,
                    vertex_client=self.vertex_client,
                    models_config=GEMINI_MODELS_CONFIG
                )
                # Handle potential tuple return from the LLM utility function
                if isinstance(llm_output, tuple):
                    model_used, analysis_text = llm_output # Unpack model name and response
                    logger.debug(f"LLM analysis completed with model: {model_used}")
                else:
                    analysis_text = llm_output
                
                logger.debug(f"LLM raw analysis to be parsed: {analysis_text}") # Log the raw response for debugging
            else:
                logger.error("LLM clients not initialized and not using mock. Cannot analyze.")
                raise ValueError("LLM clients not initialized and not using mock.")

            extracted_details = extract_analysis_details(analysis_text)
            logger.info(f"Extracted analysis for article ID: {article_id} - Importance: {extracted_details.get('importance_score')}, Sentiment: {extracted_details.get('sentiment_score')}")
            
            return {
                "article_id": article_id,
                "article_title": article_title,
                "article_source_db_name": article.get("db_name", "N/A"),
                "article_source_collection_name": article.get("collection_name", "N/A"),
                "article_url": article.get("url", "N/A"),
                "analysis_type": analysis_type,
                "analysis_raw": analysis_text,
                "analysis_structured": extracted_details,
                "analyzed_at": datetime.now()
            }
        except Exception as e:
            logger.error(f"Error querying LLM for article ID {article_id}: {e}", exc_info=True)
            return {}

    async def analyze_batch(self, articles: List[Dict], db_name: str, collection_name: str, analysis_type: str = "evening") -> List[Dict]:
        """Analyze a batch of news articles concurrently."""
        logger.info(f"Starting {analysis_type} batch analysis for {len(articles)} articles from {db_name}/{collection_name}.")
        contextual_articles = []
        for article in articles:
            article_copy = article.copy()
            article_copy['db_name'] = db_name
            article_copy['collection_name'] = collection_name
            contextual_articles.append(article_copy)
            
        tasks = [self.analyze_news_article(article, analysis_type) for article in contextual_articles]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        final_results = []
        for i, res in enumerate(results):
            if isinstance(res, Exception):
                original_article_id = contextual_articles[i].get('_id', 'Unknown ID')
                logger.error(f"Error analyzing article ID {original_article_id} in batch: {res}", exc_info=res)
                final_results.append({
                    "article_id": original_article_id,
                    "article_title": contextual_articles[i].get('title', 'N/A'),
                    "analysis_type": analysis_type,
                    "analysis_raw": "Analysis failed due to error.",
                    "analysis_structured": extract_analysis_details("Analysis failed."),
                    "analyzed_at": datetime.now(),
                    "error": str(res)
                })
            else:
                final_results.append(res)
            
            # Add a small delay to avoid hitting API rate limits
            await asyncio.sleep(1)

        logger.info(f"Finished {analysis_type} batch analysis for {db_name}/{collection_name}. Processed {len(final_results)} results.")
        return final_results

    def get_unanalyzed_news(self, db_name: str, collection_name: str, limit: int = 20, max_age_days: int = 2) -> List[Dict]:
        """Retrieve recent, unanalyzed news articles from MongoDB."""
        logger.debug(f"Fetching up to {limit} unanalyzed articles from the last {max_age_days} days from {db_name}/{collection_name}.")
        
        # Calculate the cutoff date
        cutoff_date = datetime.now() - timedelta(days=max_age_days)
        
        try:
            db = self.client[db_name]
            collection = db[collection_name]
            
            # Find articles that are not analyzed AND are recent
            query = {
                "analyzed": {"$ne": True},
                "fetched_at": {"$gte": cutoff_date} # Use fetched_at for recency check
            }
            
            news_list = list(collection.find(query).limit(limit))
            logger.info(f"Found {len(news_list)} recent, unanalyzed articles in {db_name}/{collection_name}.")
            return news_list
        except Exception as e:
            # Added specific check for a common error if 'date' field is missing from some documents
            if "fetched_at" in str(e):
                 logger.error(f"Error fetching news from {db_name}/{collection_name}. A 'fetched_at' field might be missing or in the wrong format in some documents. Query: {query}", exc_info=True)
            else:
                logger.error(f"Error fetching unanalyzed news from {db_name}/{collection_name}: {e}", exc_info=True)
            return []

    def save_analysis(self, db_name: str, collection_name: str, analysis_results: List[Dict]):
        """Save analysis results back to MongoDB."""
        if not analysis_results:
            logger.info(f"No analysis results to save for {db_name}/{collection_name}.")
            return
        logger.info(f"Saving {len(analysis_results)} analysis results to {db_name}/{collection_name}.")
        try:
            db = self.client[db_name]
            target_collection = db[collection_name]
            
            for result in analysis_results:
                if "error" in result and result.get("analysis_raw") == "Analysis failed due to error.":
                    # Handle how to save errored analyses, maybe mark as error or skip structured part
                     target_collection.update_one(
                        {"_id": result["article_id"]},
                        {
                            "$set": {
                                "analysis_error": result.get("error", "Unknown analysis error"),
                                "analyzed": True, # Or a different flag like 'analysis_attempted_with_error'
                                "analyzed_at": result["analyzed_at"]
                            }
                        }
                    )
                else:
                    target_collection.update_one(
                        {"_id": result["article_id"]},
                        {
                            "$set": {
                                "analysis_raw": result["analysis_raw"],
                                "analysis_structured": result["analysis_structured"],
                                "analyzed": True,
                                "analyzed_at": result["analyzed_at"]
                            }
                        }
                    )
            logger.info(f"Successfully saved {len(analysis_results)} results to {db_name}/{collection_name}.")
        except Exception as e:
            logger.error(f"Error saving analysis results to {db_name}/{collection_name}: {e}", exc_info=True)

    def check_alerts_and_send_emails(self, analysis_results: List[Dict], alert_thresholds: Dict):
        """
        Collects all triggered alerts, formats them into a single digest email, and sends it.
        """
        if not self.email_service:
            logger.error("EmailService not initialized. Cannot send alerts.")
            return

        alerts_to_send = []
        for result in analysis_results:
            if "error" in result:
                continue

            structured_data = result.get("analysis_structured", {})
            importance = structured_data.get("importance_score", 0)
            sentiment = structured_data.get("sentiment_score", 0)

            if importance >= alert_thresholds["IMPORTANCE_THRESHOLD"]:
                if (sentiment >= alert_thresholds["POSITIVE_SENTIMENT_THRESHOLD"] or
                        sentiment <= alert_thresholds["NEGATIVE_SENTIMENT_THRESHOLD"]):
                    alerts_to_send.append(result)

        if not alerts_to_send:
            logger.info("Finished checking alerts. No articles met the threshold for an alert.")
            return

        logger.info(f"Found {len(alerts_to_send)} articles that meet the alert threshold. Compiling digest email.")

        # Build the digest email
        email_subject = f"每日新闻分析警报 - {len(alerts_to_send)}条重要新闻"
        
        email_body_parts = [
            f"您好，\n\n系统分析发现 {len(alerts_to_send)} 条可能影响市场的重要新闻，详情如下：\n"
            "--------------------------------------------------\n"
        ]

        for alert in alerts_to_send:
            structured = alert.get("analysis_structured", {})
            title = alert.get("article_title", "N/A")
            url = alert.get("article_url", "N/A")
            importance = structured.get("importance_score", 0)
            sentiment = structured.get("sentiment_score", 0)
            sectors = ", ".join(structured.get("sectors", []))
            summary = structured.get("analysis_summary", "无摘要")

            article_html = f"""
            新闻标题: {title}
            重要性评分: {importance}/10
            市场情绪评分: {sentiment}/10
            影响板块: {sectors if sectors else "未提及"}
            
            分析摘要:
            {summary}

            新闻链接: {url}
            --------------------------------------------------
            """
            email_body_parts.append(textwrap.dedent(article_html))

        final_email_body = "\n".join(email_body_parts)

        try:
            self.email_service.send_email(
                subject=email_subject,
                body=final_email_body
            )
            logger.info(f"Successfully sent digest email with {len(alerts_to_send)} alerts.")
        except Exception as e:
            logger.error(f"Failed to send digest email: {e}", exc_info=True)


async def run_analysis_pipeline_for_sources(analyzer: NewsAnalyzer, sources: List[Tuple[str, str]], alert_thresholds: Dict, analysis_type: str, limit: int, max_age_days: int):
    """
    Runs the full analysis and alerting pipeline for a list of sources.
    """
    for db_name, collection_name in sources:
        logger.info(f"Processing source for {analysis_type} pipeline: {db_name}/{collection_name}")
        unanalyzed_news = analyzer.get_unanalyzed_news(db_name, collection_name, limit=limit, max_age_days=max_age_days) 

        if unanalyzed_news:
            logger.info(f"Found {len(unanalyzed_news)} articles in {db_name}/{collection_name} for {analysis_type} pipeline. Analyzing...")
            analysis_results = await analyzer.analyze_batch(unanalyzed_news, db_name, collection_name, analysis_type)
            
            analyzer.save_analysis(db_name, collection_name, analysis_results)
            
            # Call the new method for checking alerts
            analyzer.check_alerts_and_send_emails(analysis_results, alert_thresholds)
        else:
            logger.info(f"No recent, unanalyzed articles found in {db_name}/{collection_name}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the news analysis pipeline.")
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of articles to process per source."
    )
    parser.add_argument(
        "--days",
        type=int,
        default=2,
        help="Maximum age in days for articles to be analyzed."
    )
    args = parser.parse_args()

    logger.info(f"news_analyzer.py script started with limit={args.limit}, days={args.days}.")
    
    # Alert Thresholds are now the only separate config needed here
    ALERT_THRESHOLDS = {
        "IMPORTANCE_THRESHOLD": 7,
        "POSITIVE_SENTIMENT_THRESHOLD": 8,
        "NEGATIVE_SENTIMENT_THRESHOLD": 3,
    }

    news_analyzer_instance = NewsAnalyzer() 

    # Define sources for different analysis types
    chinese_sources = [
        ("Sina_Stock", "sina_news_roll"),
        ("Cnstock_Stock", "cnstock_news_roll"),
        ("Stcn_Stock", "stcn_news_roll"),
        ("Nbd_Stock", "nbd_news_roll"),
        ("Eastmoney_Stock", "eastmoney_kuaixun_api_news"),
        # ("Jrj_Stock", "jrj_news_company"), # Disabled in main_crawler.py
    ]
    
    us_sources = [
        ("Finnhub_News", "market_news")
    ]
    
    try:
        current_hour = datetime.now().hour
        if 8 <= current_hour < 16:
            logger.info("Starting morning analysis pipeline (US news focus)")
            asyncio.run(run_analysis_pipeline_for_sources(
                news_analyzer_instance, 
                us_sources, 
                ALERT_THRESHOLDS,
                "morning",
                limit=args.limit,
                max_age_days=args.days
            ))
        else:
            logger.info("Starting evening analysis pipeline (Chinese news focus)")
            asyncio.run(run_analysis_pipeline_for_sources(
                news_analyzer_instance, 
                chinese_sources, 
                ALERT_THRESHOLDS,
                "evening",
                limit=args.limit,
                max_age_days=args.days
            ))
    except Exception as e:
        logger.critical(f"Critical error in main execution: {e}", exc_info=True)
    
    logger.info("news_analyzer.py script finished.")

