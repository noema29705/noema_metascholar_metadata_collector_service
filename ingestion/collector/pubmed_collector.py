from typing import List
from datetime import datetime
import xml.etree.ElementTree as ET
from ingestion.colelctor.base_collector import BaseCollector
from ingestion.utils.models import Paper
import requests

class PubMedCollector(BaseCollector):
    def __init__(self, rate_limit: float = 3, api_key: str = None):
        super().__init__("PubMed", rate_limit)
        self.base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
        self.api_key = api_key
    
    def search(self, query: str, max_results: int = 1000) -> List[Paper]:
        """Search PubMed"""
        # First, search for IDs
        search_params = {
            'db': 'pubmed',
            'term': query,
            'retmax': max_results,
            'sort': 'date',
            'rettype': 'json'
        }
        
        if self.api_key:
            search_params['api_key'] = self.api_key
        
        try:
            self.rate_limiter.wait()
            search_response = self.session.get(
                f"{self.base_url}/esearch.fcgi",
                params=search_params,
                timeout=self.timeout
            ).json()
            
            pmids = search_response.get('esearchresult', {}).get('idlist', [])
            
            if not pmids:
                return []
            
            # Fetch full details
            return self._fetch_details(pmids)
        except Exception as e:
            self.logger.error(f"Error searching PubMed: {e}")
            return []
    
    def _fetch_details(self, pmids: List[str]) -> List[Paper]:
        """Fetch detailed information for PMIDs"""
        papers = []
        
        # Fetch in batches
        batch_size = 100
        for i in range(0, len(pmids), batch_size):
            batch = pmids[i:i + batch_size]
            
            params = {
                'db': 'pubmed',
                'id': ','.join(batch),
                'rettype': 'xml'
            }
            
            if self.api_key:
                params['api_key'] = self.api_key
            
            try:
                self.rate_limiter.wait()
                response = self.session.get(
                    f"{self.base_url}/efetch.fcgi",
                    params=params,
                    timeout=self.timeout
                ).text
                
                papers.extend(self.parse_response(response))
            except Exception as e:
                self.logger.warning(f"Error fetching batch: {e}")
                continue
        
        return papers
    
    def parse_response(self, xml_string: str) -> List[Paper]:
        """Parse PubMed XML response"""
        papers = []
        
        try:
            root = ET.fromstring(xml_string)
            
            for article in root.findall('.//PubmedArticle'):
                try:
                    medline = article.find('.//MedlineCitation')
                    if medline is None:
                        continue
                    
                    pmid = medline.findtext('.//PMID')
                    title = medline.findtext('.//Article/ArticleTitle')
                    abstract = medline.findtext('.//Article/Abstract/AbstractText')
                    
                    authors = []
                    for author in medline.findall('.//Author'):
                        name = author.findtext('LastName', '')
                        if name:
                            initials = author.findtext('Initials', '')
                            authors.append(f"{name} {initials}".strip())
                    
                    year_elem = medline.find('.//Article/Journal/JournalIssue/PubDate')
                    year = 0
                    if year_elem is not None:
                        year_text = year_elem.findtext('Year')
                        if year_text:
                            year = int(year_text)
                    
                    paper = Paper(
                        paper_id=pmid,
                        title=title or "Unknown",
                        abstract=abstract,
                        authors=authors,
                        year=year,
                        source="pubmed",
                        url=f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/",
                        venue=medline.findtext('.//Article/Journal/Title')
                    )
                    papers.append(paper)
                except Exception as e:
                    self.logger.warning(f"Error parsing article: {e}")
                    continue
        except ET.ParseError as e:
            self.logger.error(f"XML parsing error: {e}")
        
        return papers