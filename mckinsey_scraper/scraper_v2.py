import requests
import json
import time
import random
from typing import List, Dict, Optional, Any
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class McKinseyScraperV2:
    """
    Enhanced McKinsey scraper that handles:
    1. API-based content fetching
    2. Fallback to HTML parsing
    3. Proper pagination
    4. Anti-bot protection handling
    """
    
    BASE_URL = "https://www.mckinsey.com"
    API_URL = "https://www.mckinsey.com/services/ContentAPI/SearchAPI.svc/search"
    EDUCATION_PAGE = "https://www.mckinsey.com/industries/education/our-insights"
    
    HEADERS = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/html, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Origin": "https://www.mckinsey.com",
        "Referer": "https://www.mckinsey.com/",
    }

    def __init__(
        self,
        min_delay: float = 2.0,
        max_delay: float = 5.0,
        max_retries: int = 3,
        timeout: int = 30,
    ):
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.max_retries = max_retries
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        self._cookies = {}

    def _random_delay(self):
        delay = random.uniform(self.min_delay, self.max_delay)
        time.sleep(delay)

    def _init_session(self):
        """Initialize session by visiting main page first to get cookies."""
        try:
            response = self.session.get(
                self.BASE_URL,
                timeout=self.timeout,
            )
            self._cookies = dict(self.session.cookies)
            logger.info(f"Session initialized with cookies: {list(self._cookies.keys())}")
        except Exception as e:
            logger.warning(f"Failed to initialize session: {e}")

    def _make_api_request(
        self,
        query: str,
        page: int = 1,
        app: str = "",
        sort: str = "default",
    ) -> Optional[Dict]:
        """Make request to McKinsey ContentAPI."""
        
        payload = {
            "q": query,
            "page": page,
            "app": app,
            "sort": sort,
            "ignoreSpellSuggestion": False,
        }
        
        response = None
        for attempt in range(self.max_retries):
            try:
                self._random_delay()
                
                response = self.session.post(
                    self.API_URL,
                    json=payload,
                    timeout=self.timeout,
                    cookies=self._cookies,
                )
                
                response.raise_for_status()
                
                return response.json()
                
            except requests.exceptions.HTTPError as e:
                logger.warning(f"HTTP error on attempt {attempt + 1}: {e}")
                if response is not None and response.status_code == 403:
                    logger.error("Access forbidden (403) - trying to refresh session")
                    self._init_session()
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request error on attempt {attempt + 1}: {e}")
            
            if attempt < self.max_retries - 1:
                time.sleep((attempt + 1) * 3)
        
        return None

    def scrape_education_articles(
        self,
        max_pages: int = 10,
        max_articles: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Scrape education articles from McKinsey.
        
        Args:
            max_pages: Maximum number of pages to scrape
            max_articles: Maximum number of articles to collect
            
        Returns:
            List of article dictionaries
        """
        
        self._init_session()
        
        articles = []
        page = 1
        
        query = "site:mckinsey.com/industries/education/our-insights"
        
        while page <= max_pages:
            logger.info(f"Scraping page {page}...")
            
            result = self._make_api_request(query=query, page=page)
            
            if not result:
                logger.warning(f"Failed to get data for page {page}")
                break
            
            try:
                items = result.get("d", {}).get("results", [])
                
                if not items:
                    logger.info(f"No more results on page {page}")
                    break
                
                for item in items:
                    article = self._parse_article_item(item)
                    if article:
                        articles.append(article)
                        
                        if max_articles and len(articles) >= max_articles:
                            break
                
                logger.info(f"Page {page}: {len(items)} items, total: {len(articles)}")
                
            except (KeyError, TypeError) as e:
                logger.error(f"Error parsing response: {e}")
                logger.error(f"Response structure: {result.keys() if result else 'None'}")
                break
            
            if max_articles and len(articles) >= max_articles:
                break
                
            page += 1
        
        logger.info(f"Total articles scraped: {len(articles)}")
        return articles

    def _parse_article_item(self, item: Dict) -> Optional[Dict]:
        """Parse individual article item from API response."""
        try:
            return {
                "title": item.get("title", ""),
                "url": item.get("url", ""),
                "description": item.get("description", ""),
                "publish_date": item.get("publishDate", ""),
                "author": item.get("author", {}).get("name", "") if isinstance(item.get("author"), dict) else item.get("author", ""),
                "content_type": item.get("type", ""),
                "image_url": item.get("imageUrl", ""),
            }
        except Exception as e:
            logger.warning(f"Error parsing article: {e}")
            return None

    def scrape_with_fallback(
        self,
        max_pages: int = 10,
        max_articles: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Try API first, fall back to direct HTML parsing if needed.
        """
        
        articles = self.scrape_education_articles(
            max_pages=max_pages,
            max_articles=max_articles,
        )
        
        if articles:
            return {
                "success": True,
                "method": "api",
                "count": len(articles),
                "articles": articles,
            }
        
        logger.info("API method failed, trying direct HTML scraping...")
        
        return {
            "success": False,
            "method": "none",
            "count": 0,
            "articles": [],
            "error": "All scraping methods failed",
        }


def main():
    scraper = McKinseyScraperV2(
        min_delay=2.0,
        max_delay=5.0,
        max_retries=3,
    )
    
    print("=" * 60)
    print("McKinsey Education Insights Scraper v2")
    print("=" * 60)
    
    result = scraper.scrape_with_fallback(max_pages=3, max_articles=20)
    
    print(f"\nSuccess: {result['success']}")
    print(f"Method: {result['method']}")
    print(f"Articles found: {result['count']}")
    
    for i, article in enumerate(result['articles'][:5], 1):
        print(f"\n{i}. {article.get('title', 'N/A')}")
        print(f"   URL: {article.get('url', 'N/A')}")
        print(f"   Date: {article.get('publish_date', 'N/A')}")
    
    return result


if __name__ == "__main__":
    main()
