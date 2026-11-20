from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def resolve_path(path: str | Path, base: Path | None = None) -> Path:
    """Relative paths resolve against the project root (or ``base``)."""
    p = Path(path)
    return p if p.is_absolute() else (base or PROJECT_ROOT) / p
