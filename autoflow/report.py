"""Human-readable rendering of profiles and runs."""
from __future__ import annotations

from typing import Any, Dict


def render_profile(p: Dict[str, Any]) -> str:
    lines = [f"Profile: {p['dataset']}",
             f"  rows loaded: {p['rows_loaded']}  columns: {p['columns_count']}  "
             f"malformed records: {p['malformed_records']}  duplicate rows: {p['duplicate_rows']}",
             f"  {p['note']}"]
    if p.get("candidate_keys"):
        lines.append(f"  candidate keys (observed, unconfirmed): {', '.join(p['candidate_keys'])}")
    lines.append("")
    hdr = f"  {'column':<22}{'type':<10}{'conf':<8}{'role':<12}{'null%':>7}{'unique':>9}  notes"
    lines += [hdr, "  " + "-" * (len(hdr) - 2)]
    for c, x in p["columns"].items():
        notes = []
        if x["invalid_for_inferred_type"]:
            notes.append(f"{x['invalid_for_inferred_type']} value(s) don't fit type")
        if x["leading_zeros"]:
            notes.append("leading zeros kept (string)")
        if x["whitespace_anomalies"]:
            notes.append(f"{x['whitespace_anomalies']} whitespace")
        if x.get("potential_outliers"):
            notes.append(f"{x['potential_outliers']} outlier(s)")
        if x["is_constant"]:
            notes.append("constant")
        if x.get("date_order") in ("ambiguous", "conflict", "dmy", "mdy"):
            notes.append(f"date order: {x['date_order']}")
        lines.append(f"  {c[:21]:<22}{x['inferred_type']:<10}{x['confidence_label']:<8}{x['possible_role']:<12}"
                     f"{x['null_pct'] * 100:>6.1f}%{x['unique_count']:>9}  {'; '.join(notes)}")
    return "\n".join(lines)


def render_run(r: Dict[str, Any]) -> str:
    L = [f"AutoFlow run {r['run_id']}", f"  dataset: {r['dataset_name']}   mode: {r['mode']}   status: {r['status']}",
         f"  can_proceed: {r['can_proceed']}" + (f"   would_proceed: {r['would_proceed']}" if r["mode"] != "gate" else ""),
         f"  rows processed: {r['rows_processed']}  approved: {r['rows_approved']}  quarantined: {r['rows_quarantined']}"
         f"  blocked: {r['rows_blocked']}"]
    if r["mode"] != "gate":
        L.append(f"  assessment only: would approve {r['rows_would_approve']}, would quarantine {r['rows_would_quarantine']}")
    L.append(f"  issues: {r['issues_detected']}  recovery proposals: {r['recovery_proposals']}  applied: "
             f"{r['recovery_actions_applied']}  revalidation passed/failed: {r['revalidation_passed']}/{r['revalidation_failed']}")
    if r.get("source_changed_since_last_run") is not None:
        L.append(f"  source changed since last run: {r['source_changed_since_last_run']}")
    if r.get("idempotent_replay"):
        L.append(f"  idempotent replay of run {r.get('replay_of')}: output not duplicated")
    if r.get("output"):
        L.append(f"  output: {r['output']}")
    for e in r["errors"]:
        L.append(f"  ERROR: {e}")
    for w in r["warnings"]:
        L.append(f"  note: {w}")
    if r["issues"]:
        L.append("\n  Issues:")
        for i in r["issues"]:
            L.append(f"   [{i['severity']:<7}|{i['kind']:<9}] {i['message']}")
            if i.get("probable_cause"):
                L.append(f"        probable cause: {i['probable_cause']}")
            if i.get("recommendation"):
                L.append(f"        next step: {i['recommendation']}")
    acts = [p for p in r["proposals"] if p["action"] != "none"]
    if acts:
        L.append("\n  Recovery proposals:")
        for p in acts[:25]:
            L.append(f"   {p['proposal_id']} cat {p['category']} {p['rule_id']} row {p['row_id']} col {p['column']}: "
                     f"{p['original']!r} -> {p['proposed']!r}  [{p['authorization']} / {p['status']}]")
        if len(acts) > 25:
            L.append(f"   ... {len(acts) - 25} more")
    return "\n".join(L)
