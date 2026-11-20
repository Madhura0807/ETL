"""Environment-variable helpers (credentials never live in YAML or code)."""
import os
import re
from pathlib import Path

_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def expand_env(value):
    """Recursively replace ``${VAR}`` in strings/dicts/lists. Missing variable -> KeyError."""
    if isinstance(value, str):
        def repl(match):
            name = match.group(1)
            if name not in os.environ:
                raise KeyError(f"environment variable '{name}' is not set")
            return os.environ[name]
        return _PATTERN.sub(repl, value)
    if isinstance(value, dict):
        return {k: expand_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [expand_env(v) for v in value]
    return value


def load_dotenv_file(path: str | Path) -> None:
    """Minimal .env reader (KEY=VALUE). Existing environment variables win."""
    p = Path(path)
    if not p.exists():
        return
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))
