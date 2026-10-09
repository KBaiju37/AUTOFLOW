"""Core contracts shared by every AutoFlow component.

Nothing in here knows about a specific source, pipeline tool or business domain.
"""
from __future__ import annotations

import abc
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional

import pandas as pd


# --------------------------------------------------------------------------- errors
class AutoFlowError(Exception):
    """Base class for all AutoFlow errors."""


class ConfigError(AutoFlowError):
    """Invalid configuration or rules."""


class SourceError(AutoFlowError):
    """A source could not be read or validated."""


class DependencyMissing(SourceError):
    """An optional dependency required by a connector is not installed."""


class QualityGateFailed(AutoFlowError):
    """Raised by the ``gate`` decorator when a dataset may not proceed."""

    def __init__(self, result: "RunResult"):
        super().__init__(
            f"AutoFlow blocked dataset '{result.dataset_name}' "
            f"(status={result.status}, run_id={result.run_id})"
        )
        self.result = result


# --------------------------------------------------------------------------- vocab
class Severity:
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class Kind:
    CONFIRMED = "confirmed"   # violates an explicit rule / structural fact
    SUSPECTED = "suspected"   # statistically or heuristically suspicious
    INFERRED = "inferred"     # a recommendation derived from profiling


class RunStatus:
    SUCCESS = "SUCCESS"
    SUCCESS_WITH_WARNINGS = "SUCCESS_WITH_WARNINGS"
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS"
    FAILED = "FAILED"
    DRY_RUN = "DRY_RUN"


class Mode:
    GATE = "gate"          # in-memory gate; recovery/output only if enabled in config
    DRY_RUN = "dry_run"    # assess + propose only; nothing applied, nothing written
    MONITOR = "monitor"    # observe only; no recovery, no destination writes


MODES = (Mode.GATE, Mode.DRY_RUN, Mode.MONITOR)


# --------------------------------------------------------------------------- data
@dataclass
class Dataset:
    """Normalised in-memory representation of a batch of records.

    ``frame.index`` holds the *source row id* (1-based record number for files,
    the caller's index for DataFrames) and is preserved through the whole run.
    """

    frame: pd.DataFrame
    name: str = "dataset"
    source_type: str = "unknown"
    source_id: str = ""
    fingerprint: str = ""
    malformed: List[Dict[str, Any]] = field(default_factory=list)  # {row_id, reason, raw}
    duplicate_columns: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)
    total_rows_in_source: Optional[int] = None  # may exceed len(frame) when limited


@dataclass
class ConnectorInfo:
    type: str
    description: str
    dependencies: List[str]
    dependencies_available: bool
    capabilities: Dict[str, bool]
    status: str


class SourceConnector(abc.ABC):
    """Contract every source connector implements. Connectors never modify the source."""

    type: str = "abstract"
    description: str = ""
    dependencies: tuple = ()
    status: str = "implemented"
    capabilities: Dict[str, bool] = {
        "chunked": False,
        "sampling": True,
        "read_only": True,
        "stable_row_ids": True,
    }

    def __init__(self, config: Dict[str, Any]):
        self.config = dict(config or {})

    def validate(self) -> List[str]:
        """Return a list of problems (empty if the source looks usable). No side effects."""
        return []

    @abc.abstractmethod
    def read(self, limit: Optional[int] = None) -> Dataset:
        """Read up to ``limit`` records (all if None)."""

    def iter_chunks(self, chunksize: int = 50_000) -> Iterator[pd.DataFrame]:
        """Yield DataFrames (indexed by source row id). Default: one chunk."""
        yield self.read().frame

    def count_rows(self) -> Optional[int]:
        return None

    def info(self) -> ConnectorInfo:
        from .util import dependency_available

        avail = all(dependency_available(d) for d in self.dependencies)
        return ConnectorInfo(
            self.type, self.description, list(self.dependencies), avail,
            dict(self.capabilities), self.status if avail else "dependency-missing",
        )


# --------------------------------------------------------------------------- findings
@dataclass
class Issue:
    code: str
    severity: str
    kind: str
    message: str
    scope: str = "dataset"            # dataset | column | row
    column: Optional[str] = None
    count: int = 0
    sample_row_ids: List[Any] = field(default_factory=list)
    probable_cause: Optional[str] = None
    recommendation: Optional[str] = None
    source: str = "generic"           # generic | user | baseline | plugin:<name>

    def to_dict(self) -> Dict[str, Any]:
        from .util import jsonable
        return {k: jsonable(v) for k, v in self.__dict__.items()}


@dataclass
class RowViolation:
    row_id: Any
    column: Optional[str]
    check: str
    value: Any
    severity: str = Severity.ERROR
    detail: Dict[str, Any] = field(default_factory=dict)
    source: str = "user"


@dataclass
class RecoveryProposal:
    proposal_id: str
    rule_id: str
    rule_version: str
    category: str                       # A | B | C
    row_id: Any
    column: Optional[str]
    original: Any
    proposed: Any
    action: str                         # set_value | drop_row | none
    reason: str
    reversible: bool = True
    changes_business_meaning: bool = False
    requires_approval: bool = False
    authorization: str = "pending"      # authorized | needs_approval | unresolved | recovery_disabled | rejected
    status: str = "proposed"            # proposed | applied | pending_approval | unresolved | rejected | failed_revalidation | revalidated
    result: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        from .util import jsonable
        return {k: jsonable(v) for k, v in self.__dict__.items()}


@dataclass
class RunResult:
    run_id: str
    dataset_name: str
    status: str
    mode: str
    run_key: str = ""
    fingerprint: str = ""
    rows_processed: int = 0
    rows_approved: int = 0
    rows_quarantined: int = 0
    rows_blocked: int = 0
    rows_would_approve: int = 0
    rows_would_quarantine: int = 0
    issues_detected: int = 0
    recovery_proposals: int = 0
    recovery_actions_applied: int = 0
    revalidation_passed: int = 0
    revalidation_failed: int = 0
    can_proceed: bool = False
    would_proceed: bool = False
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    issues: List[Issue] = field(default_factory=list)
    proposals: List[RecoveryProposal] = field(default_factory=list)
    diagnosis: List[Dict[str, Any]] = field(default_factory=list)
    quarantine_summary: List[Dict[str, Any]] = field(default_factory=list)
    output: Dict[str, Any] = field(default_factory=dict)
    source_changed_since_last_run: Optional[bool] = None
    idempotent_replay: bool = False
    replay_of: Optional[str] = None
    started_at: str = ""
    ended_at: str = ""
    duration_s: float = 0.0
    profile: Dict[str, Any] = field(default_factory=dict)
    approved_data: Optional[pd.DataFrame] = None
    quarantined_data: Optional[pd.DataFrame] = None

    def to_dict(self, include_profile: bool = True) -> Dict[str, Any]:
        from .util import jsonable
        d: Dict[str, Any] = {}
        for k, v in self.__dict__.items():
            if k in ("approved_data", "quarantined_data"):
                continue
            if k == "profile" and not include_profile:
                continue
            if k in ("issues", "proposals"):
                d[k] = [x.to_dict() for x in v]
            else:
                d[k] = jsonable(v)
        return d
