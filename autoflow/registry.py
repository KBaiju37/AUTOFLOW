"""Connector registry: add a source by registering a ``SourceConnector`` subclass."""
from __future__ import annotations

import os
from typing import Any, Dict, List, Type

from .contracts import ConfigError, SourceConnector


class ConnectorRegistry:
    def __init__(self) -> None:
        self._classes: Dict[str, Type[SourceConnector]] = {}
        self._loaded = False

    def _ensure(self) -> None:
        if not self._loaded:
            self._loaded = True
            from . import sources  # noqa: F401  (registers built-ins on import)

    def register(self, cls: Type[SourceConnector]) -> Type[SourceConnector]:
        if not getattr(cls, "type", None) or cls.type == "abstract":
            raise ValueError("connector class must define a unique 'type'")
        self._classes[cls.type] = cls
        return cls

    def types(self) -> List[str]:
        self._ensure()
        return sorted(self._classes)

    def create(self, type_: str, config: Dict[str, Any]) -> SourceConnector:
        self._ensure()
        if type_ not in self._classes:
            raise ConfigError(f"Unknown source type '{type_}'. Known types: {sorted(self._classes)}")
        return self._classes[type_](config)

    def describe(self) -> List[Dict[str, Any]]:
        self._ensure()
        out = []
        for t in sorted(self._classes):
            inst = self._classes[t]({})
            i = inst.info()
            out.append({"type": i.type, "status": i.status, "dependencies": i.dependencies,
                        "dependencies_available": i.dependencies_available,
                        "capabilities": i.capabilities, "description": i.description})
        return out


registry = ConnectorRegistry()

_EXT = {
    ".csv": ("csv", {}), ".tsv": ("csv", {"delimiter": "\t"}), ".txt": ("csv", {}),
    ".psv": ("csv", {"delimiter": "|"}),
    ".json": ("json", {}), ".jsonl": ("jsonl", {}), ".ndjson": ("jsonl", {}),
    ".xlsx": ("excel", {}), ".xlsm": ("excel", {}),
    ".parquet": ("parquet", {}),
    ".sqlite": ("sqlite", {}), ".sqlite3": ("sqlite", {}), ".db": ("sqlite", {}),
}


def connector_for_path(path: str, **overrides: Any) -> SourceConnector:
    """Pick a connector from a file extension (``discovery``)."""
    ext = os.path.splitext(path)[1].lower()
    if ext not in _EXT:
        raise ConfigError(f"Unsupported file type '{ext}'. Supported extensions: {sorted(_EXT)}")
    t, defaults = _EXT[ext]
    cfg = {"path": path, **defaults, **overrides}
    return registry.create(t, cfg)
