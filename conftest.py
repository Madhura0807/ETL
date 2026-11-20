import pytest
from sqlalchemy import create_engine

from src.config_loader import DEFAULT_SETTINGS, deep_merge, validate_pipeline_config


@pytest.fixture
def settings(tmp_path):
    return deep_merge(DEFAULT_SETTINGS, {
        "paths": {k: str(tmp_path / k) for k in ("schemas", "reports", "logs", "quarantine", "processed")},
        "logging": {"level": "WARNING", "console": False},
        "defaults": {"retry": {"max_attempts": 3, "backoff_seconds": 0, "backoff_factor": 1}},
    })


@pytest.fixture
def engine(tmp_path):
    eng = create_engine(f"sqlite:///{tmp_path / 'target.db'}")
    yield eng
    eng.dispose()


@pytest.fixture
def no_sleep():
    return lambda seconds: None


@pytest.fixture
def make_config(settings):
    """make_config(**sections) -> validated pipeline config for a CSV source."""
    def _make(source=None, **sections):
        raw = {
            "name": "test_pipeline",
            "source": source or {"type": "csv", "path": "unused.csv"},
            "target": {"type": "mysql", "table": "people"},
            "quality_checks": {"primary_key": "id"},
            "load": {"mode": "upsert", "batch_size": 2},
        }
        raw.update(sections)
        return validate_pipeline_config(raw, settings)
    return _make
