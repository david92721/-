import requests
import json
import time
import random
from typing import List, Dict, Optional, Any
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class McKinseyEducationScraper:
    BASE_URL = "https://www.mckinsey.com"
    API_URL = "https://www.mckinsey.com/services/ContentAPI/SearchAPI.svc/search"
    
    DEFAULT_HEADERS = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/html, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Origin": "https://www.mckinsey.com",
        "Referer": "https://www.mckinsey.com/industries/education/our-insights",
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
        self.session.headers.update(self.DEFAULT_HEADERS)

    def _random_delay(self):
        delay = random.uniform(self.min_delay, self.max_delay)
        logger.debug(f"Waiting {delay:.2f} seconds before next request")
        time.sleep(delay)

    def _make_request(
        self,
        url: str,
        method: str = "POST",
        data: Optional[Dict] = None,
        params: Optional[Dict] = None,
    ) -> Optional[Dict]:
        response = None
        for attempt in range(self.max_retries):
            try:
                self._random_delay()
                
                if method == "POST":
                    response = self.session.post(
                        url,
                        json=data,
                        params=params,
                        timeout=self.timeout,
                    )
                else:
                    response = self.session.get(
                        url,
                        params=params,
                        timeout=self.timeout,
                    )

                response.raise_for_status()
                
                if "application/json" in response.headers.get("Content-Type", ""):
                    return response.json()
                else:
                    return {"raw_html": response.text}
                    
            except requests.exceptions.HTTPError as e:
                logger.warning(f"HTTP error on attempt {attempt + 1}: {e}")
                if response is not None and response.status_code == 403:
                    logger.error("Access forbidden (403) - may be blocked")
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request error on attempt {attempt + 1}: {e}")
            
            if attempt < self.max_retries - 1:
                wait_time = (attempt + 1) * 2
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
        
        return None

    def scrape_education_insights(
        self,
        max_pages: Optional[int] = None,
        max_articles: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        articles = []
        page = 1
        
        search_query = "site:mckinsey.com/industries/education"
        
        while True:
            if max_pages and page > max_pages:
                logger.info(f"Reached max pages limit: {max_pages}")
                break
                
            if max_articles and len(articles) >= max_articles:
                logger.info(f"Reached max articles limit: {max_articles}")
                break

            logger.info(f"Scraping page {page}...")
            
            request_data = {
                "q": search_query,
                "page": page,
                "app": "",
                "sort": "default",
                "ignoreSpellSuggestion": False,
            }

            result = self._make_request(self.API_URL, method="POST", data=request_data)
            
            if not result:
                logger.warning(f"Failed to get data for page {page}")
                break

            try:
                results_data = result.get("data", {}).get("results", [])
                
                if not results_data:
                    logger.info(f"No more results on page {page}")
                    break
                
                for item in results_data:
                    article = self._parse_article(item)
                    if article:
                        articles.append(article)
                        
                        if max_articles and len(articles) >= max_articles:
                            break
                
                logger.info(f"Found {len(results_data)} items on page {page}, total: {len(articles)}")
                
            except (KeyError, TypeError) as e:
                logger.error(f"Error parsing response: {e}")
                break

            page += 1

        logger.info(f"Total articles scraped: {len(articles)}")
        return articles

    def _parse_article(self, item: Dict) -> Optional[Dict]:
        try:
            return {
                "title": item.get("title", ""),
                "url": item.get("url", ""),
                "summary": item.get("description", item.get("summary", "")),
                "published_date": item.get("publishDate", item.get("date", "")),
                "author": item.get("author", item.get("authors", "")),
                "type": item.get("type", item.get("contentType", "")),
                "image_url": item.get("imageUrl", item.get("thumbnail", "")),
            }
        except Exception as e:
            logger.warning(f"Error parsing article: {e}")
            return None

    def scrape_with_url(self, url: str) -> Dict[str, Any]:
        logger.info(f"Scraping URL: {url}")
        
        response = self._make_request(url, method="GET")
        
        if not response:
            return {"error": "Failed to fetch URL"}
        
        return response

    def get_education_page_count(self) -> int:
        request_data = {
            "q": "site:mckinsey.com/industries/education",
            "page": 1,
            "app": "",
            "sort": "default",
            "ignoreSpellSuggestion": False,
        }
        
        result = self._make_request(self.API_URL, method="POST", data=request_data)
        
        if result:
            try:
                total = result.get("data", {}).get("total", 0)
                return total
            except (KeyError, TypeError):
                pass
        
        return 0


def main():
    scraper = McKinseyEducationScraper(
        min_delay=2.0,
        max_delay=5.0,
        max_retries=3,
        timeout=30,
    )
    
    print("=" * 60)
    print("McKinsey Education Insights Scraper")
    print("=" * 60)
    
    total = scraper.get_education_page_count()
    print(f"Total articles available: {total}")
    
    articles = scraper.scrape_education_insights(max_pages=5)
    
    print(f"\nScraped {len(articles)} articles:")
    print("-" * 60)
    
    for i, article in enumerate(articles, 1):
        print(f"\n{i}. {article.get('title', 'N/A')}")
        print(f"   URL: {article.get('url', 'N/A')}")
        print(f"   Date: {article.get('published_date', 'N/A')}")
        print(f"   Type: {article.get('type', 'N/A')}")
    
    return articles


if __name__ == "__main__":
    main()
