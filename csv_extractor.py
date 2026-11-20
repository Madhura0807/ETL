from __future__ import annotations

import csv

import numpy as np
import pandas as pd

from src.exceptions import ExtractionError
from src.extractors.base import BaseExtractor
from src.utils.paths import resolve_path

NA_STRINGS = {"", "NA", "N/A", "n/a", "NULL", "null", "NaN", "nan", "#N/A"}


class CSVExtractor(BaseExtractor):
    """Reads everything as text (types are inferred later by the schema stage).

    Malformed rows - any row whose field count differs from the header - are NOT loaded into the
    dataframe but captured in ``rejected`` so they can be quarantined instead of silently lost.
    Empty fields and common NA markers (see NA_STRINGS) become nulls.
    """

    source_type = "csv"

    def _extract(self):
        path = resolve_path(self.config["path"])
        if not path.exists():
            raise ExtractionError(f"CSV file not found: {path}")
        delimiter = self.config.get("delimiter", ",")
        rejected: list[dict] = []
        rows: list[list[str]] = []
        try:
            with open(path, newline="", encoding=self.config.get("encoding", "utf-8")) as fh:
                reader = csv.reader(fh, delimiter=delimiter)
                header = next(reader, None)
                if not header or not any(h.strip() for h in header):
                    raise ExtractionError(f"CSV file has no header/content: {path}")
                header[0] = header[0].lstrip("\ufeff")
                for rec in reader:
                    if not rec:
                        continue  # blank line
                    if len(rec) != len(header):
                        rejected.append({
                            "reason": f"malformed CSV row (line {reader.line_num}): "
                                      f"{len(rec)} fields, header has {len(header)}",
                            "record": delimiter.join(rec)})
                    else:
                        rows.append(rec)
        except UnicodeDecodeError as exc:
            raise ExtractionError(f"CSV file is not valid {self.config.get('encoding', 'utf-8')}: {path}") from exc
        except csv.Error as exc:
            raise ExtractionError(f"CSV parse error in {path}: {exc}") from exc

        seen: dict[str, int] = {}
        columns = []
        for h in header:  # duplicate header names would otherwise overwrite each other
            n = seen.get(h, 0)
            seen[h] = n + 1
            columns.append(h if n == 0 else f"{h}.{n}")
        df = pd.DataFrame(rows, columns=columns, dtype=object)
        df = df.map(lambda v: np.nan if v in NA_STRINGS else v)
        return df, rejected, {"path": str(path), "file_size_bytes": path.stat().st_size}
