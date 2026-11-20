"""Per-run logging: console + a log file named after the pipeline and execution id."""
import logging
from pathlib import Path

LOGGER_NAME = "etl"


class _RunContext(logging.Filter):
    def __init__(self, pipeline: str, execution_id: str):
        super().__init__()
        self.pipeline, self.execution_id = pipeline, execution_id

    def filter(self, record):
        record.pipeline = self.pipeline
        record.execution_id = self.execution_id[:8]
        return True


def setup_logging(settings: dict, logs_dir: Path, pipeline: str, execution_id: str) -> tuple[logging.Logger, Path]:
    """Configure the ``etl`` logger for one run. Handlers from earlier runs are removed first."""
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(getattr(logging, settings["logging"]["level"].upper(), logging.INFO))
    logger.propagate = False
    for h in list(logger.handlers):
        logger.removeHandler(h)
        h.close()
    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(pipeline)s | %(execution_id)s | %(name)s | %(message)s")
    ctx = _RunContext(pipeline, execution_id)
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = logs_dir / f"{pipeline}_{execution_id[:8]}.log"
    handlers = [logging.FileHandler(log_path, encoding="utf-8")]
    if settings["logging"].get("console", True):
        handlers.append(logging.StreamHandler())
    for h in handlers:
        h.setFormatter(fmt)
        h.addFilter(ctx)
        logger.addHandler(h)
    return logger, log_path
