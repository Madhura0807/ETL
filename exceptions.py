"""Framework-specific exceptions so callers can distinguish failure categories."""


class ETLError(Exception):
    """Base class for all framework errors."""


class ConfigError(ETLError):
    """Pipeline or settings configuration is invalid."""

    def __init__(self, errors):
        self.errors = [errors] if isinstance(errors, str) else list(errors)
        super().__init__("; ".join(self.errors))


class ExtractionError(ETLError):
    """A source could not be read or returned an unusable payload."""


class LoadError(ETLError):
    """Loading into the target failed."""


class SchemaMismatchError(LoadError):
    """Dataframe columns do not match the target table (never silently ignored)."""


class ReferenceDataError(ETLError):
    """A table needed for a referential-integrity check is unavailable."""
