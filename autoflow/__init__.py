"""AutoFlow - a reliability layer for existing data pipelines."""
from .contracts import (  # noqa: F401
    ConfigError, Dataset, Issue, Mode, QualityGateFailed, RecoveryProposal, RunResult, RunStatus,
    SourceConnector, SourceError,
)
from .engine import AutoFlow  # noqa: F401

__version__ = "0.1.0"
