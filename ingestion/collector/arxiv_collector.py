from typing import List, Optional, Dict
import feedparser
from datetime import datetime
from urllib.parse import urlencode
import time
import random
import json
from pathlib import Path

from ingestion.collector.base_collector import BaseCollector
from ingestion.utils.models import Paper


class ArxivCollector(BaseCollector):
    def __init__(self, rate_limit: float = 3, config: Optional[Dict] = None):
        super().__init__("ArXiv", rate_limit)
        self.base_url = "http://export.arxiv.org/api/query"
        self.config = config or {}

        pagination_cfg = self.config.get("pagination", {})
        self.pagination_enabled = pagination_cfg.get("enabled", True)
        self.default_start = int(pagination_cfg.get("start", 0))
        self.default_page_size = int(pagination_cfg.get("page_size", 1000))
        self.max_requests = int(pagination_cfg.get("max_requests", 1000))
        self.resume_enabled = bool(pagination_cfg.get("resume_from_checkpoint", True))
        self.checkpoint_every_requests = int(pagination_cfg.get("checkpoint_every_requests", 10))

        self.request_delay_seconds = float(self.config.get("request_delay_seconds", 0))
        self.request_delay_jitter_seconds = float(self.config.get("request_delay_jitter_seconds", 0))

        checkpoint_path = pagination_cfg.get("checkpoint_file", "data/raw/arxiv/arxiv_checkpoint.json")
        self.checkpoint_file = Path(checkpoint_path)
        self.checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
        self.checkpoint = self._load_checkpoint()

    def _load_checkpoint(self) -> Dict:
        if not self.checkpoint_file.exists():
            return {"queries": {}}
        try:
            with self.checkpoint_file.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if "queries" not in data:
                data["queries"] = {}
            return data
        except Exception as e:
            self.logger.warning(f"Could not load checkpoint file: {e}")
            return {"queries": {}}

    def _save_checkpoint(self):
        tmp_file = self.checkpoint_file.with_suffix(".tmp")
        with tmp_file.open("w", encoding="utf-8") as f:
            json.dump(self.checkpoint, f, indent=2)
        tmp_file.replace(self.checkpoint_file)

    def _checkpoint_key(self, query: str) -> str:
        return query.strip()

    def _get_resume_start(self, query: str) -> int:
        if not self.resume_enabled:
            return self.default_start
        key = self._checkpoint_key(query)
        return int(self.checkpoint.get("queries", {}).get(key, self.default_start))

    def _set_resume_start(self, query: str, start: int):
        key = self._checkpoint_key(query)
        self.checkpoint.setdefault("queries", {})[key] = int(start)

    def _mark_query_complete(self, query: str):
        key = self._checkpoint_key(query)
        self.checkpoint.setdefault("queries", {})[key] = -1

    def search(self, query: str, max_results: int = 1000, categories: Optional[List[str]] = None) -> List[Paper]:
        papers: List[Paper] = []
        request_count = 0
        page_size = min(max_results, self.default_page_size)

        queries = []
        if categories:
            for category in categories:
                cat_query = f"cat:{category}"
                if query:
                    cat_query = f"({cat_query}) AND ({query})"
                queries.append(cat_query)
        else:
            queries.append(query)

        for single_query in queries:
            start = self._get_resume_start(single_query)
            if start == -1:
                self.logger.info(f"Skipping completed query: {single_query}")
                continue

            while True:
                if request_count >= self.max_requests:
                    self.logger.info(f"Reached max_requests={self.max_requests}, saving checkpoint and stopping.")
                    self._save_checkpoint()
                    return papers

                batch = self._perform_search(single_query, start=start, max_results=page_size)
                request_count += 1

                if not batch:
                    self.logger.info(f"No more results for query: {single_query}")
                    self._mark_query_complete(single_query)
                    self._save_checkpoint()
                    break

                papers.extend(batch)

                next_start = start + len(batch)
                self._set_resume_start(single_query, next_start)

                if request_count % self.checkpoint_every_requests == 0:
                    self._save_checkpoint()

                if len(batch) < page_size:
                    self.logger.info(f"Reached last page for query: {single_query}")
                    self._mark_query_complete(single_query)
                    self._save_checkpoint()
                    break

                if not self.pagination_enabled:
                    self._save_checkpoint()
                    break

                start += page_size
                self._sleep_between_requests()

        self._save_checkpoint()
        return papers

    def _sleep_between_requests(self):
        if self.request_delay_seconds <= 0:
            return
        jitter = random.uniform(0, self.request_delay_jitter_seconds) if self.request_delay_jitter_seconds > 0 else 0
        time.sleep(self.request_delay_seconds + jitter)

    def _perform_search(self, query: str, start: int, max_results: int) -> List[Paper]:
        params = {
            "search_query": query,
            "start": start,
            "max_results": max_results,
            "sortBy": "submittedDate",
            "sortOrder": "descending",
        }

        try:
            self.rate_limiter.wait()
            url = f"{self.base_url}?{urlencode(params)}"
            feed = feedparser.parse(url)
            return self.parse_response(feed)
        except Exception as e:
            self.logger.error(f"Error searching ArXiv at start={start}: {e}")
            return []

    def parse_response(self, feed) -> List[Paper]:
        papers = []

        for entry in getattr(feed, "entries", []):
            try:
                paper = Paper(
                    paper_id=entry.id.split("/abs/")[-1],
                    title=entry.title,
                    abstract=getattr(entry, "summary", None),
                    authors=[author.name for author in getattr(entry, "authors", [])],
                    year=int(entry.published[:4]),
                    source="arxiv",
                    url=entry.id,
                    publication_date=datetime.fromisoformat(entry.published.replace("Z", "+00:00")),
                    venue="ArXiv",
                )
                papers.append(paper)
            except Exception as e:
                self.logger.warning(f"Error parsing ArXiv entry: {e}")
                continue

        return papers