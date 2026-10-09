"""Connectors for data that already lives in the caller's process."""
from __future__ import annotations

import importlib
from typing import Any, List, Optional

from ..adapters import to_dataset
from ..contracts import ConfigError, Dataset, SourceConnector, SourceError
from ..registry import registry
from ..util import frame_fingerprint


@registry.register
class DataFrameSource(SourceConnector):
    type = "dataframe"
    description = "A pandas DataFrame passed programmatically (config key 'dataframe'). Not usable from YAML."

    def validate(self) -> List[str]:
        return [] if "dataframe" in self.config else ["programmatic only: pass config {'dataframe': df}"]

    def read(self, limit: Optional[int] = None) -> Dataset:
        errs = self.validate()
        if errs:
            raise SourceError("; ".join(errs))
        ds = to_dataset(self.config["dataframe"], self.config.get("name", "dataframe"))
        if limit:
            ds.frame = ds.frame.head(limit)
        ds.fingerprint = frame_fingerprint(ds.frame)
        return ds


@registry.register
class CallableSource(SourceConnector):
    """Calls an existing extract function. From YAML: callable: 'package.module:function' plus allow_import: true."""
    type = "callable"
    description = "Zero-argument callable returning a DataFrame/records/Arrow table. Import-path form needs allow_import: true."

    def _resolve(self):
        c = self.config.get("callable")
        if callable(c):
            return c
        if isinstance(c, str):
            if not self.config.get("allow_import"):
                raise ConfigError("Importing a callable from config requires allow_import: true (it runs your code)")
            mod, _, fn = c.partition(":")
            if not mod or not fn:
                raise ConfigError("callable must look like 'package.module:function'")
            try:
                return getattr(importlib.import_module(mod), fn)
            except (ImportError, AttributeError) as exc:
                raise ConfigError(f"Cannot import '{c}': {exc}") from exc
        raise ConfigError("'callable' is required")

    def validate(self) -> List[str]:
        try:
            self._resolve()
            return []
        except ConfigError as exc:
            return [str(exc)]

    def read(self, limit: Optional[int] = None) -> Dataset:
        ds = to_dataset(self._resolve()(), self.config.get("name", "callable"))
        if limit:
            ds.frame = ds.frame.head(limit)
        ds.fingerprint = frame_fingerprint(ds.frame)
        return ds
