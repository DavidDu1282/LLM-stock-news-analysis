from typing import Dict, List, Optional
import asyncio
from datetime import datetime
from pymongo import MongoClient
from llm_old import send_query_to_first_available_model, models_config

class NewsAnalyzer:
    def __init__(self, mongo_uri: str = "mongodb://localhost:27017/"):
        self.client = MongoClient(mongo_uri)
        
    async def analyze_news_article(self, article: Dict) -> Dict:
        """Analyze a single news article using Gemini LLM."""
        prompt = f"""Analyze this financial news article and provide the following information:
        1. Importance (High/Medium/Low)
        2. Main stocks mentioned and their potential impact
        3. Affected sectors
        4. Key points and implications
        5. Market sentiment (Positive/Negative/Neutral)

        Article Title: {article.get('title', '')}
        Article Content: {article.get('content', '')}
        Publication Date: {article.get('date', '')}
        Source: {article.get('source', '')}

        Please provide a structured analysis."""

        analysis = await send_query_to_first_available_model(query=prompt, models_config=models_config)
        
        return {
            "article_id": article.get("_id"),
            "analysis": analysis,
            "analyzed_at": datetime.now()
        }

    async def analyze_batch(self, articles: List[Dict]) -> List[Dict]:
        """Analyze a batch of news articles concurrently."""
        tasks = [self.analyze_news_article(article) for article in articles]
        return await asyncio.gather(*tasks)

    def get_unanalyzed_news(self, db_name: str, collection_name: str, limit: int = 10) -> List[Dict]:
        """Retrieve unanalyzed news articles from MongoDB."""
        db = self.client[db_name]
        collection = db[collection_name]
        
        # Find articles that haven't been analyzed yet
        return list(collection.find(
            {"analyzed": {"$ne": True}},
            {"_id": 1, "title": 1, "content": 1, "date": 1, "source": 1}
        ).limit(limit))

    def save_analysis(self, db_name: str, collection_name: str, analysis_results: List[Dict]):
        """Save analysis results back to MongoDB."""
        db = self.client[db_name]
        collection = db[collection_name]
        
        for result in analysis_results:
            collection.update_one(
                {"_id": result["article_id"]},
                {
                    "$set": {
                        "analysis": result["analysis"],
                        "analyzed": True,
                        "analyzed_at": result["analyzed_at"]
                    }
                }
            )

async def main():
    analyzer = NewsAnalyzer()
    
    # List of news sources to analyze
    sources = [
        ("Sina_Stock", "sina_news_company"),
        ("Jrj_Stock", "jrj_news_company"),
        ("Cnstock_Stock", "cnstock_news_company"),
        ("Stcn_Stock", "stcn_news_company")
    ]
    
    for db_name, collection_name in sources:
        # Get unanalyzed news
        unanalyzed_news = analyzer.get_unanalyzed_news(db_name, collection_name)
        
        if unanalyzed_news:
            # Analyze the news
            analysis_results = await analyzer.analyze_batch(unanalyzed_news)
            
            # Save results
            analyzer.save_analysis(db_name, collection_name, analysis_results)
            
            print(f"Analyzed {len(analysis_results)} articles from {db_name}/{collection_name}")

if __name__ == "__main__":
    asyncio.run(main())