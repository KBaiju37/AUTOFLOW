"""AutoFlow demonstrations on synthetic data (generated here; nothing external is downloaded).

  Demo 1  CSV    - IoT sensor readings (unfamiliar schema, no rules -> profile + generic checks, then rules)
  Demo 2  SQLite - shipments table (different schema and different problems), with recovery
  Demo 3  An 'existing ETL' function that calls AutoFlow and continues only if the result allows it

Run:  python demo/run_demo.py
"""
import os
import sqlite3
import sys
import tempfile

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from autoflow import AutoFlow                                   # noqa: E402
from autoflow.registry import registry                          # noqa: E402
from autoflow.report import render_profile, render_run          # noqa: E402


def make_sensor_csv(path):
    rows = []
    for i in range(1, 61):
        rows.append({"reading_id": f"R{i:04d}", "device": f"dev-{i % 4}", "temp_c": f"{20 + (i % 7) * 0.5:.1f}",
                     "taken_at": f"2024-03-{(i % 28) + 1:02d} 10:00:00", "state": "ok" if i % 5 else "warn"})
    rows[3]["temp_c"] = "N/A"                    # non-numeric value
    rows[10]["temp_c"] = "1,234.5"               # thousands separator; even after normalising (1234.5) it is out of range, so it stays quarantined
    rows[20]["temp_c"] = " 22.0 "                # surrounding whitespace
    rows[30]["temp_c"] = "9999"                  # statistical outlier (legitimate? unknown) - never auto-rejected
    rows[40]["reading_id"] = rows[39]["reading_id"]   # duplicate id with different data
    rows[50]["taken_at"] = "13/45/2024"          # garbage date
    pd.DataFrame(rows).to_csv(path, index=False)


def make_shipments_db(path):
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE shipments (shipment_no TEXT, dest_zip TEXT, weight_kg REAL, carrier TEXT, status TEXT)")
    data = [(f"S{i:05d}", f"{(i * 37) % 100000:05d}", round(1 + (i % 9) * 2.5, 1),
             ["DHL", "UPS", "FEDEX"][i % 3], ["delivered", "in_transit", "created"][i % 3]) for i in range(1, 41)]
    data[2] = ("S00003", "01234", -4.0, "DHL", "delivered")        # negative weight: invalid for this domain
    data[7] = ("S00008", "90210", 3.0, "ups", "in_transit")        # carrier casing (needs approval to fix)
    data[12] = ("S00013", "02134", 5.0, "UPS", "Delivered")        # status casing
    data[15] = ("S00016", "55555", None, "FEDEX", "created")       # missing weight
    con.executemany("INSERT INTO shipments VALUES (?,?,?,?,?)", data)
    con.commit()
    con.close()


def banner(t):
    print("\n" + "=" * 78 + f"\n{t}\n" + "=" * 78)


def main():
    work = tempfile.mkdtemp(prefix="autoflow_demo_")
    csv_path, db_path = os.path.join(work, "sensors.csv"), os.path.join(work, "logistics.db")
    make_sensor_csv(csv_path)
    make_shipments_db(db_path)

    banner("Demo 1a - CSV: unfamiliar dataset, NO rules (profile + conservative generic checks)")
    af = AutoFlow()
    csv_src = registry.create("csv", {"path": csv_path})
    print(render_profile(af.profile(csv_src, "sensors")))
    r = af.validate(csv_src, "sensors")
    print("\n" + render_run(r.to_dict(include_profile=False)))

    banner("Demo 1b - same CSV with rules + recovery enabled (category A only) + partial success allowed")
    rules1 = {"columns": {
        "reading_id": {"type": "string", "nullable": False, "unique": True},
        "temp_c": {"type": "number", "nullable": False, "min": -50, "max": 60},
        "taken_at": {"type": "datetime", "nullable": False},
        "state": {"allowed": ["ok", "warn"]}}}
    af1 = AutoFlow({"recovery": {"enabled": True}, "validation": {"allow_partial_success": True}}, rules=rules1)
    r = af1.validate(csv_src, "sensors")
    print(render_run(r.to_dict(include_profile=False)))
    print("\nQuarantine (original values + reasons):")
    print(r.quarantined_data[["reading_id", "temp_c", "taken_at", "_quarantine_reasons"]].to_string())

    banner("Demo 2 - SQLite table: different schema, different problems; category B needs approval")
    db_src = registry.create("sqlite", {"path": db_path, "table": "shipments"})
    rules2 = {"columns": {
        "shipment_no": {"type": "string", "unique": True, "nullable": False},
        "dest_zip": {"type": "string", "pattern": "\\d{5}"},        # leading zeros are preserved
        "weight_kg": {"type": "number", "min": 0, "nullable": False},
        "carrier": {"allowed": ["DHL", "UPS", "FEDEX"]},
        "status": {"allowed": ["created", "in_transit", "delivered"]}}}
    af2 = AutoFlow({"recovery": {"enabled": True}, "validation": {"allow_partial_success": True}}, rules=rules2)
    r = af2.validate(db_src, "shipments")
    print(render_run(r.to_dict(include_profile=False)))
    print("\nNow approving rule 'match_allowed_case' (explicit policy)...")
    af2b = AutoFlow({"recovery": {"enabled": True, "approved_rules": ["match_allowed_case"]},
                     "validation": {"allow_partial_success": True}}, rules=rules2)
    r = af2b.validate(db_src, "shipments")
    print(f"status={r.status} approved={r.rows_approved} quarantined={r.rows_quarantined} "
          f"applied={r.recovery_actions_applied} revalidated_ok={r.revalidation_passed}")

    banner("Demo 3 - an existing ETL step that calls AutoFlow and obeys the returned policy")
    af3 = AutoFlow({"validation": {"allow_partial_success": False}}, rules=rules2)

    def existing_etl(extract):
        df = extract()                                   # E
        df["weight_lb"] = df["weight_kg"]                # T (the company's own transformation, untouched)
        result = af3.validate(df, "shipments")           # AutoFlow gate, one call
        if result.can_proceed:                           # L only if allowed
            print(f"  loading {len(result.approved_data)} rows")
        else:
            print(f"  NOT loading. status={result.status}; {result.rows_quarantined} row(s) quarantined; "
                  f"first diagnosis: {result.diagnosis[0]['symptom']}")
        return result
    existing_etl(lambda: db_src.read().frame)
    clean = db_src.read().frame
    clean = clean[clean.index.isin([1, 2, 4, 5, 6, 9, 10, 11])]
    existing_etl(lambda: clean.copy())

    banner("Dry-run and monitor semantics")
    r = af2.validate(db_src, "shipments", mode="dry_run")
    print(f"dry_run: status={r.status} can_proceed={r.can_proceed} would_proceed={r.would_proceed} "
          f"rows_approved={r.rows_approved} (assessment only: would approve {r.rows_would_approve}, "
          f"would quarantine {r.rows_would_quarantine}); proposals={r.recovery_proposals}; applied={r.recovery_actions_applied}")
    print(f"\nSource files untouched; demo workspace: {work}")


if __name__ == "__main__":
    main()
