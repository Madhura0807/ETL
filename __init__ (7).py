from src.exceptions import ConfigError
from src.extractors.api_extractor import APIExtractor
from src.extractors.base import BaseExtractor, ExtractionResult
from src.extractors.csv_extractor import CSVExtractor
from src.extractors.json_extractor import JSONExtractor
from src.extractors.sql_extractor import SQLExtractor

EXTRACTORS = {"csv": CSVExtractor, "json": JSONExtractor, "api": APIExtractor, "sql": SQLExtractor}


def build_extractor(source_config: dict, retry_config: dict | None = None, logger=None, sleep=None) -> BaseExtractor:
    """Factory: new source types only need a new class registered here."""
    try:
        cls = EXTRACTORS[source_config["type"]]
    except KeyError as exc:
        raise ConfigError(f"unsupported source type: {source_config.get('type')}") from exc
    return cls(source_config, retry_config, logger, sleep)


__all__ = ["build_extractor", "ExtractionResult", "BaseExtractor", "EXTRACTORS"]
