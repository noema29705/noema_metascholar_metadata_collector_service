from typing import List
from datetime import datetime
from ingestion.colelctor.base_collector import BaseCollector
from ingestion.utils.models import Paper

class OpenAlexCollector(BaseCollector):
    def __init__(self, rate_limit: float = 10):
        super().__init__("OpenAlex", rate_limit)
        self.base_url = "https://api.openalex.org/works"
    
    def search(self, query: str, max_results: int = 200) -> List[Paper]:
        """Search OpenAlex"""
        params = {
            'search': query,
            'per_page': min(max_results, 200),
            'sort': 'cited_by_count:desc'
        }
        
        try:
            response = self._fetch(self.base_url, params)
            return self.parse_response(response)
        except Exception as e:
            self.logger.error(f"Error searching OpenAlex: {e}")
            return []
    
    def parse_response(self, response) -> List[Paper]:
        """Parse OpenAlex response"""
        papers = []
        
        for result in response.get('results', []):
            try:
                authors = [
                    author['author']['display_name'] 
                    for author in result.get('authorships', [])
                ]
                
                pub_date = None
                if result.get('publication_date'):
                    pub_date = datetime.fromisoformat(result['publication_date'])
                
                paper = Paper(
                    paper_id=result['id'].split('/')[-1],
                    title=result.get('title', 'Unknown'),
                    abstract=result.get('abstract_inverted_index'),
                    authors=authors,
                    year=result.get('publication_year', 0),
                    source="openalex",
                    url=result.get('url', ''),
                    publication_date=pub_date,
                    citations_count=result.get('cited_by_count'),
                    venue=result.get('primary_location', {}).get('source', {}).get('display_name'),
                    doi=result.get('doi')
                )
                papers.append(paper)
            except Exception as e:
                self.logger.warning(f"Error parsing OpenAlex result: {e}")
                continue
        
        return papers