"""Compare the current inferred schema with the saved baseline and decide what may happen next.

Classification (severity) is a property of the change; the *action* is a configurable policy.

| change            | severity                                         | policy key        | default  |
|-------------------|--------------------------------------------------|-------------------|----------|
| column added      | WARNING                                          | column_added      | exclude  |
| column removed    | CRITICAL                                         | column_removed    | block    |
| type: int->float  | WARNING (compatible widening)                    | (always allowed)  | allow    |
| type: float->int  | INFO (values still fit a float column)           | (always allowed)  | allow    |
| type: other       | CRITICAL                                         | type_changed      | block    |
| nullable: no->yes | WARNING                                          | nullable_changed  | allow    |
| nullable: yes->no | INFO                                             | (always allowed)  | allow    |

``exclude`` = load proceeds without the new column; ``add`` = target table gets the column;
``block`` = nothing is loaded and the baseline is NOT updated.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass

from src.utils.time import utcnow

SEVERITY_ORDER = {"INFO": 1, "WARNING": 2, "CRITICAL": 3}
STATUS_BY_SEVERITY = {"INFO": "INFORMATIONAL", "WARNING": "WARNING", "CRITICAL": "CRITICAL"}


@dataclass
class SchemaChange:
    change_type: str      # column_added | column_removed | type_changed | nullable_changed
    column: str
    severity: str         # INFO | WARNING | CRITICAL
    previous: str | None
    current: str | None
    action: str           # allow | exclude | add | block
    message: str
    overridden: bool = False


@dataclass
class DriftReport:
    status: str                       # NONE | INFORMATIONAL | WARNING | CRITICAL | BASELINE_CREATED
    decision: str                     # proceed | block
    previous_version: int | None
    changes: list[SchemaChange]
    exclude_columns: list[str]
    add_columns: list[str]
    blocked_reasons: list[str]
    detected_at: str

    @property
    def blocked(self) -> bool:
        return self.decision == "block"

    @property
    def has_changes(self) -> bool:
        return bool(self.changes)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["summary"] = {sev: sum(1 for c in self.changes if c.severity == sev) for sev in SEVERITY_ORDER}
        return d


def _type_change_severity(prev: str, cur: str) -> tuple[str, bool]:
    """Return (severity, always_allowed)."""
    if prev == "integer" and cur == "float":
        return "WARNING", True
    if prev == "float" and cur == "integer":
        return "INFO", True
    return "CRITICAL", False


def detect_drift(previous: dict | None, current: dict, policies: dict, accept_drift: bool = False,
                 previous_version: int | None = None) -> DriftReport:
    now = utcnow().isoformat()
    if previous is None:
        return DriftReport("BASELINE_CREATED", "proceed", None, [], [], [], [], now)

    prev_cols, cur_cols = previous["columns"], current["columns"]
    changes: list[SchemaChange] = []

    for col in cur_cols:
        if col not in prev_cols:
            changes.append(SchemaChange("column_added", col, "WARNING", None, cur_cols[col]["type"],
                                        policies["column_added"], f"new column '{col}' ({cur_cols[col]['type']})"))
    for col in prev_cols:
        if col not in cur_cols:
            changes.append(SchemaChange("column_removed", col, "CRITICAL", prev_cols[col]["type"], None,
                                        policies["column_removed"], f"column '{col}' disappeared from the source"))
    for col in cur_cols.keys() & prev_cols.keys():
        p, c = prev_cols[col], cur_cols[col]
        if p["type"] != c["type"] and "unknown" not in (p["type"], c["type"]):
            sev, always = _type_change_severity(p["type"], c["type"])
            changes.append(SchemaChange("type_changed", col, sev, p["type"], c["type"],
                                        "allow" if always else policies["type_changed"],
                                        f"column '{col}' type changed {p['type']} -> {c['type']}"))
        if p["nullable"] != c["nullable"]:
            if c["nullable"]:
                changes.append(SchemaChange("nullable_changed", col, "WARNING", "NOT NULL", "NULLABLE",
                                            policies["nullable_changed"], f"column '{col}' now contains nulls"))
            else:
                changes.append(SchemaChange("nullable_changed", col, "INFO", "NULLABLE", "NOT NULL",
                                            "allow", f"column '{col}' no longer contains nulls"))

    changes.sort(key=lambda ch: (-SEVERITY_ORDER[ch.severity], ch.change_type, ch.column))

    blocked_reasons: list[str] = []
    for ch in changes:
        if ch.action == "block":
            if accept_drift:  # operator explicitly accepted: blocked adds become real columns
                ch.overridden = True
                ch.action = "add" if ch.change_type == "column_added" else "allow"
            else:
                blocked_reasons.append(ch.message)

    status = "NONE"
    if changes:
        status = STATUS_BY_SEVERITY[max((c.severity for c in changes), key=SEVERITY_ORDER.get)]
    return DriftReport(
        status=status,
        decision="block" if blocked_reasons else "proceed",
        previous_version=previous_version,
        changes=changes,
        exclude_columns=sorted(c.column for c in changes if c.change_type == "column_added" and c.action == "exclude"),
        add_columns=sorted(c.column for c in changes if c.change_type == "column_added" and c.action == "add"),
        blocked_reasons=blocked_reasons,
        detected_at=now,
    )
