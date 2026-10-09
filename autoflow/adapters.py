"""Pipeline adapters: translate an existing pipeline's data object into a ``Dataset`` and back.

Adapters hold no quality or recovery logic; they only convert. To support a new
pipeline technology, subclass ``PipelineAdapter`` and call ``register_adapter``.
"""
from __future__ import annotations

import abc
from typing import Any, Callable, List

import pandas as pd

from .contracts import ConfigError, Dataset, RunResult, SourceConnector
from .typeops import normalize_frame


class PipelineAdapter(abc.ABC):
    name = "abstract"

    @abc.abstractmethod
    def accepts(self, obj: Any) -> bool: ...

    @abc.abstractmethod
    def to_dataset(self, obj: Any, name: str) -> Dataset: ...

    def from_result(self, result: RunResult, original: Any) -> Any:
        """What the pipeline gets back as 'approved data'. Default: the approved DataFrame."""
        return result.approved_data


def _from_frame(df: pd.DataFrame, name: str, source_type: str) -> Dataset:
    if not df.index.is_unique:
        df = df.reset_index(drop=True)
    frame, dups = normalize_frame(df)
    frame.index.name = "_row_id"
    return Dataset(frame, name=name, source_type=source_type, source_id=name, duplicate_columns=dups)


class DataFrameAdapter(PipelineAdapter):
    name = "pandas"

    def accepts(self, obj: Any) -> bool:
        return isinstance(obj, pd.DataFrame)

    def to_dataset(self, obj: pd.DataFrame, name: str) -> Dataset:
        return _from_frame(obj, name, "dataframe")


class RecordsAdapter(PipelineAdapter):
    name = "records"

    def accepts(self, obj: Any) -> bool:
        return isinstance(obj, list) and all(isinstance(r, dict) for r in obj)

    def to_dataset(self, obj: List[dict], name: str) -> Dataset:
        cols: List[str] = []
        for r in obj:
            for k in r:
                if k not in cols:
                    cols.append(k)
        df = pd.DataFrame({str(c): [r.get(c) for r in obj] for c in cols}, dtype=object,
                          index=pd.Index(range(1, len(obj) + 1)))
        return _from_frame(df, name, "records")


class PyArrowAdapter(PipelineAdapter):
    name = "pyarrow"

    def accepts(self, obj: Any) -> bool:
        return type(obj).__name__ == "Table" and type(obj).__module__.startswith("pyarrow")

    def to_dataset(self, obj: Any, name: str) -> Dataset:
        return _from_frame(obj.to_pandas(), name, "pyarrow")

    def from_result(self, result: RunResult, original: Any) -> Any:
        import pyarrow as pa
        return None if result.approved_data is None else pa.Table.from_pandas(result.approved_data, preserve_index=False)


class DatasetAdapter(PipelineAdapter):
    name = "dataset"

    def accepts(self, obj: Any) -> bool:
        return isinstance(obj, Dataset)

    def to_dataset(self, obj: Dataset, name: str) -> Dataset:
        return obj


class SourceAdapter(PipelineAdapter):
    name = "source"

    def accepts(self, obj: Any) -> bool:
        return isinstance(obj, SourceConnector)

    def to_dataset(self, obj: SourceConnector, name: str) -> Dataset:
        ds = obj.read()
        ds.name = name
        return ds


class CallableAdapter(PipelineAdapter):
    """Wraps a zero-argument function returning any other supported object (an existing extract step)."""
    name = "callable"

    def accepts(self, obj: Any) -> bool:
        return callable(obj) and not isinstance(obj, (pd.DataFrame, SourceConnector))

    def to_dataset(self, obj: Callable[[], Any], name: str) -> Dataset:
        return to_dataset(obj(), name)


_ADAPTERS: List[PipelineAdapter] = [
    DatasetAdapter(), DataFrameAdapter(), SourceAdapter(), RecordsAdapter(), PyArrowAdapter(), CallableAdapter(),
]


def register_adapter(adapter: PipelineAdapter, first: bool = True) -> None:
    _ADAPTERS.insert(0 if first else len(_ADAPTERS), adapter)


def find_adapter(obj: Any) -> PipelineAdapter:
    for a in _ADAPTERS:
        if a.accepts(obj):
            return a
    raise ConfigError(f"No adapter accepts objects of type {type(obj).__name__}. "
                      f"Supported: DataFrame, list of dicts, pyarrow Table, SourceConnector, callable; "
                      f"or register your own PipelineAdapter.")


def to_dataset(obj: Any, name: str) -> Dataset:
    ds = find_adapter(obj).to_dataset(obj, name)
    ds.name = name
    return ds
