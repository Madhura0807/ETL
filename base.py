"""Common extractor contract: every extractor returns the same ExtractionResult."""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

import pandas as pd

from src.utils.time import utcnow


@dataclass
class ExtractionResult:
    data: pd.DataFrame
    metadata: dict
    rejected: list[dict] = field(default_factory=list)  # {"reason": str, "record": str}


class BaseExtractor(ABC):
    source_type = "base"

    def __init__(self, source_config: dict, retry_config: dict | None = None,
                 logger: logging.Logger | None = None, sleep=None):
        self.config = source_config
        self.retry = retry_config or {"max_attempts": 3, "backoff_seconds": 1.0, "backoff_factor": 2.0}
        self.log = logger or logging.getLogger("etl.extract")
        self._sleep = sleep

    @abstractmethod
    def _extract(self) -> tuple[pd.DataFrame, list[dict], dict]:
        """Return (dataframe, rejected_records, extra_metadata)."""

    def extract(self) -> ExtractionResult:
        started = utcnow()
        df, rejected, extra = self._extract()
        df = df.reset_index(drop=True)
        metadata = {
            "source_type": self.source_type,
            "extracted_at": started.isoformat(),
            "row_count": int(len(df)),
            "column_count": int(len(df.columns)),
            "rejected_count": len(rejected),
            **extra,
        }
        self.log.info("Extracted %d rows, %d columns, %d rejected from %s source",
                      len(df), len(df.columns), len(rejected), self.source_type)
        return ExtractionResult(df, metadata, rejected)
