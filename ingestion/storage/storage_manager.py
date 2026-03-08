import json
from pathlib import Path
from typing import List
import gzip
from ingestion.utils.models import Paper
from ingestion.utils.logger import setup_logger

class StorageManager:
    def __init__(self, base_dir: str = "./data/raw"):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.logger = setup_logger("storage")
    
    def save_papers(self, papers: List[Paper], source: str, format: str = "json", compress: bool = True):
        """Save papers to disk"""
        if not papers:
            self.logger.warning(f"No papers to save for source: {source}")
            return
        
        source_dir = self.base_dir / source
        source_dir.mkdir(parents=True, exist_ok=True)
        
        filename = source_dir / f"papers_{len(list(source_dir.glob('*.json*')))}.json"
        
        data = [paper.to_dict() for paper in papers]
        
        if compress:
            with gzip.open(f"{filename}.gz", 'wt', encoding='utf-8') as f:
                json.dump(data, f, indent=2, default=str)
            self.logger.info(f"Saved {len(papers)} papers to {filename}.gz")
        else:
            with open(filename, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            self.logger.info(f"Saved {len(papers)} papers to {filename}")
    
    def load_papers(self, source: str) -> List[Paper]:
        """Load papers from disk"""
        papers = []
        source_dir = self.base_dir / source
        
        if not source_dir.exists():
            return papers
        
        for file_path in source_dir.glob("*.json*"):
            try:
                if str(file_path).endswith('.gz'):
                    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                        data = json.load(f)
                else:
                    with open(file_path) as f:
                        data = json.load(f)
                
                for item in data:
                    paper = Paper(**item)
                    papers.append(paper)
            except Exception as e:
                self.logger.error(f"Error loading {file_path}: {e}")
        
        return papers
    
    def get_stats(self) -> dict:
        """Get overall statistics"""
        stats = {}
        for source_dir in self.base_dir.iterdir():
            if source_dir.is_dir():
                files = list(source_dir.glob("*.json*"))
                total_papers = 0
                total_size = 0
                
                for file in files:
                    total_size += file.stat().st_size
                    try:
                        if str(file).endswith('.gz'):
                            with gzip.open(file, 'rt') as f:
                                total_papers += len(json.load(f))
                        else:
                            with open(file) as f:
                                total_papers += len(json.load(f))
                    except:
                        pass
                
                stats[source_dir.name] = {
                    'papers': total_papers,
                    'size_mb': total_size / (1024 * 1024),
                    'files': len(files)
                }
        
        return stats