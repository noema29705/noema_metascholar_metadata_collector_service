from dataclasses import dataclass
from typing import Optional, List
from datetime import datetime

@dataclass
class Paper:
    paper_id: str
    title: str
    abstract: Optional[str]
    authors: List[str]
    year: int
    source: str  # arxiv, pubmed, etc.
    url: str
    publication_date: Optional[datetime] = None
    citations_count: Optional[int] = None
    full_text: Optional[str] = None
    venue: Optional[str] = None
    keywords: Optional[List[str]] = None
    doi: Optional[str] = None
    categories: Optional[List[str]] = None
    arxiv_id: Optional[str] = None
    
    def to_dict(self):
        return {
            'paper_id': self.paper_id,
            'title': self.title,
            'abstract': self.abstract,
            'authors': self.authors,
            'year': self.year,
            'source': self.source,
            'url': self.url,
            'publication_date': self.publication_date.isoformat() if self.publication_date else None,
            'citations_count': self.citations_count,
            'full_text': self.full_text,
            'venue': self.venue,
            'keywords': self.keywords,
            'doi': self.doi,
            'categories': self.categories,
            'arxiv_id': self.arxiv_id,
        }