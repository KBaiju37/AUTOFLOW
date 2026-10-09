"""Command line: python -m autoflow.cli {profile,validate,run,report,connectors}

Exit codes: 0 ok / may proceed, 1 blocked or failed, 2 configuration or usage error.
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import List, Optional

from .config import Config
from .contracts import AutoFlowError, ConfigError, Mode
from .engine import AutoFlow
from .registry import connector_for_path, registry
from .report import render_profile, render_run
from .store import Store


def _connector(a):
    extra = {}
    if getattr(a, "delimiter", None):
        extra["delimiter"] = "\t" if a.delimiter == "\\t" else a.delimiter
    if getattr(a, "table", None):
        extra["table"] = a.table
    if getattr(a, "sheet", None):
        extra["sheet"] = a.sheet
    if getattr(a, "records_key", None):
        extra["records_key"] = a.records_key
    c = connector_for_path(a.input, **extra)
    errs = c.validate()
    if errs:
        raise ConfigError("; ".join(errs))
    return c


def _emit(obj, as_json: bool, text: str) -> None:
    print(json.dumps(obj, indent=2, default=str) if as_json else text)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="autoflow", description="AutoFlow data pipeline reliability layer")
    sub = p.add_subparsers(dest="cmd", required=True)

    def inp(sp):
        sp.add_argument("--input", required=True)
        sp.add_argument("--delimiter")
        sp.add_argument("--table", help="table/view name for SQLite inputs")
        sp.add_argument("--sheet")
        sp.add_argument("--records-key", dest="records_key")
        sp.add_argument("--json", action="store_true", help="machine-readable output")

    sp = sub.add_parser("profile", help="infer schema and profile an unknown dataset")
    inp(sp)
    sp.add_argument("--sample-size", type=int)
    sp = sub.add_parser("validate", help="validate (no writes); exit 1 if the data may not proceed")
    inp(sp)
    sp.add_argument("--rules")
    sp.add_argument("--config")
    sp = sub.add_parser("run", help="run a configured integration (dry run unless --commit/--enable-recovery)")
    sp.add_argument("--config", required=True)
    sp.add_argument("--dry-run", action="store_true")
    sp.add_argument("--commit", action="store_true", help="gate mode: write configured output")
    sp.add_argument("--enable-recovery", action="store_true", help="gate mode with recovery enabled")
    sp.add_argument("--json", action="store_true")
    sp = sub.add_parser("report", help="show a stored run")
    sp.add_argument("--run-id", required=True)
    sp.add_argument("--db")
    sp.add_argument("--config")
    sp.add_argument("--json", action="store_true")
    sub.add_parser("connectors", help="list connectors and their status").add_argument("--json", action="store_true")
    return p


def main(argv: Optional[List[str]] = None) -> int:
    a = build_parser().parse_args(argv)
    try:
        if a.cmd == "connectors":
            rows = registry.describe()
            _emit(rows, a.json, "\n".join(f"{r['type']:<10}{r['status']:<24}deps={r['dependencies']} {r['description']}"
                                          for r in rows))
            return 0
        if a.cmd == "profile":
            af = AutoFlow({"profiling": {"sample_size": a.sample_size}} if a.sample_size else None)
            prof = af.profile(_connector(a), a.input.rsplit("/", 1)[-1])
            _emit(prof, a.json, render_profile(prof))
            return 0
        if a.cmd == "validate":
            af = AutoFlow(a.config, rules=a.rules) if a.config else AutoFlow(None, rules=a.rules)
            r = af.validate(_connector(a), a.input.rsplit("/", 1)[-1], mode=Mode.GATE)
            _emit(r.to_dict(include_profile=False), a.json, render_run(r.to_dict(include_profile=False)))
            return 0 if r.can_proceed else 1
        if a.cmd == "run":
            raw = Config.load(a.config).data
            if a.enable_recovery:
                raw["recovery"]["enabled"] = True
            af = AutoFlow(Config.load(_override(raw)))
            mode = Mode.GATE if (a.commit or a.enable_recovery) else Mode.DRY_RUN
            if a.dry_run:
                mode = Mode.DRY_RUN
            r = af.run(mode=mode)
            d = r.to_dict(include_profile=False)
            _emit(d, a.json, render_run(d))
            return 0 if (r.can_proceed or (mode == Mode.DRY_RUN and r.would_proceed)) else 1
        if a.cmd == "report":
            if a.db:
                store = Store(a.db)
            else:
                cfg = Config.load(a.config)
                if not cfg["audit"]["enabled"]:
                    raise ConfigError("Audit is not enabled in this config; pass --db")
                store = Store(cfg["audit"]["path"])
            run = store.get_run(a.run_id)
            if run is None:
                raise ConfigError(f"Unknown run id '{a.run_id}'")
            payload = {"run": run, "events": store.events(a.run_id), "recovery_log": store.recovery_log(a.run_id)}
            text = render_run(run) + "\n\n  Audit events:\n" + "\n".join(
                f"   {e['ts']} {e['event']}" for e in payload["events"])
            _emit(payload, a.json, text)
            return 0
    except AutoFlowError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 2


def _override(data):
    # Config.load wants user-level input; data already merged, so strip nothing and reuse as dict
    return data


if __name__ == "__main__":
    sys.exit(main())
