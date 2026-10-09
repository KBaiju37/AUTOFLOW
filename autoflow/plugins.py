"""Optional domain plugins. The generic engine never contains domain rules; plugins add them on request.

A plugin is enabled only by name in config (``plugins: [retail_invoices]``) or passed to ``AutoFlow(plugins=[...])``,
and it declares when it ``applies`` so it is skipped for unrelated datasets.
"""
from __future__ import annotations

import abc
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd

from .contracts import ConfigError, Issue, Kind, RowViolation, Severity
from .typeops import as_str, missing_mask


class DomainPlugin(abc.ABC):
    name = "abstract"
    description = ""

    def applies(self, df: pd.DataFrame, dataset_name: str) -> bool:
        return True

    def rules(self) -> Optional[Dict[str, Any]]:
        """Optional rule fragment (same format as user rules). User rules always override it."""
        return None

    def check(self, df: pd.DataFrame) -> Tuple[List[Issue], List[RowViolation]]:
        return [], []


_PLUGINS: Dict[str, DomainPlugin] = {}


def register_plugin(plugin: DomainPlugin) -> DomainPlugin:
    _PLUGINS[plugin.name] = plugin
    return plugin


def resolve_plugins(items: List[Union[str, DomainPlugin]]) -> List[DomainPlugin]:
    out = []
    for it in items:
        if isinstance(it, DomainPlugin):
            out.append(it)
        elif it in _PLUGINS:
            out.append(_PLUGINS[it])
        else:
            raise ConfigError(f"Unknown plugin '{it}'. Registered: {sorted(_PLUGINS)}")
    return out


def available_plugins() -> List[str]:
    return sorted(_PLUGINS)


class RetailInvoicesPlugin(DomainPlugin):
    """Example of dataset-specific logic (invoice/quantity/price retail exports such as UCI Online Retail).

    Demonstrates plugin-style domain rules: cancellation invoices ('C...') legitimately carry negative quantities.
    """
    name = "retail_invoices"
    description = "Retail invoice rules: non-cancellation lines need Quantity > 0; UnitPrice must be >= 0."
    REQUIRED = {"InvoiceNo", "Quantity", "UnitPrice"}

    def applies(self, df: pd.DataFrame, dataset_name: str) -> bool:
        return self.REQUIRED <= set(df.columns)

    def check(self, df):
        inv = as_str(df["InvoiceNo"].fillna(""))
        qty = pd.to_numeric(as_str(df["Quantity"].fillna("")), errors="coerce")
        price = pd.to_numeric(as_str(df["UnitPrice"].fillna("")), errors="coerce")
        cancel = inv.str.upper().str.startswith("C")
        issues: List[Issue] = []
        viol: List[RowViolation] = []
        for rid in df.index[(qty <= 0) & ~cancel]:
            viol.append(RowViolation(rid, "Quantity", "retail_quantity", df.at[rid, "Quantity"]))
        for rid in df.index[price < 0]:
            viol.append(RowViolation(rid, "UnitPrice", "retail_unit_price", df.at[rid, "UnitPrice"]))
        nq = int(((qty <= 0) & ~cancel).sum())
        npc = int((price < 0).sum())
        if nq:
            issues.append(Issue("retail_quantity_violation", Severity.ERROR, Kind.CONFIRMED,
                                f"{nq} non-cancellation line(s) have Quantity <= 0", "row", "Quantity", nq,
                                [int(x) if hasattr(x, "item") else x for x in df.index[(qty <= 0) & ~cancel][:5]]))
        if npc:
            issues.append(Issue("retail_unit_price_violation", Severity.ERROR, Kind.CONFIRMED,
                                f"{npc} line(s) have negative UnitPrice", "row", "UnitPrice", npc))
        if int(cancel.sum()):
            issues.append(Issue("retail_cancellations", Severity.INFO, Kind.INFERRED,
                                f"{int(cancel.sum())} cancellation invoice line(s); negative quantities are legitimate there",
                                "dataset", "InvoiceNo", int(cancel.sum())))
        return issues, viol


register_plugin(RetailInvoicesPlugin())
