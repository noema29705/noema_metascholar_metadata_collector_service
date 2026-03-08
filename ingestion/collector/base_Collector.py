from abc import ABC, abstractmethod
from typing import List, Optional
import requests
from ingestion.utils.rate_limiter import RateLimiter
from ingestion.utils.models import Paper
from ingestion.utils.logger import setup_logger

class BaseCollector(ABC):
    def __init__(self, name: str, rate_limit: float, timeout: int = 30):
        self.name = name
        self.rate_limiter = RateLimiter(rate_limit)
        self.timeout = timeout
        self.logger = setup_logger(f"collector.{name.lower()}")
        self.session = requests.Session()
    
    @abstractmethod
    def search(self, query: str, max_results: int = 100) -> List[Paper]:
        """Search for papers matching the query"""
        pass
    
    @abstractmethod
    def parse_response(self, response) -> List[Paper]:
        """Parse API response into Paper objects"""
        pass
    
    def _fetch(self, url: str, params: dict = None, headers: dict = None) -> dict:
        """Fetch data from API with rate limiting and error handling"""
        self.rate_limiter.wait()
        
        try:
            response = self.session.get(
                url, 
                params=params, 
                headers=headers, 
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching {url}: {e}")
            raise
    
    def close(self):
        self.session.close()