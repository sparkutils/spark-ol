#!/usr/bin/env python3
"""Assert the per-construct column-lineage verdict against the captured
Databricks OpenLineage events for the pandas-operator e2e run (kanban t_38f39e29).

The machine-readable half of the evidence: the verdict table in README.md is
generated from what this script measures, not eyeballed. Run:

    python3 docs/evidence/t_38f39e29/assert_events.py

Exit 0 = every expectation held on a real DBR 17.3 / Spark 4.0.0 / Scala 2.13
cluster; non-zero prints the divergences.

Note on dataset names: UC dataset names in the events are storage UUIDs; the
readable catalog.schema.table lives in the symlinks facet. The helpers below
resolve through that map, same as the t_comprehensive_e2e / t_bfa6430b scripts.
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DEMO = "databricks_ws.openlineage_demo"
RAW = DEMO + ".raw_customers"

# The three columns every pandas cell selects from raw_customers.
ALL_IN = ("customer_id", "name", "country")


def load(name):
    events = []
    with open(os.path.join(HERE, name)) as f:
        for line in f:
            line = line.strip()
            if line:
                events.append(json.loads(line))
    return events


def symlink_names(events):
    """UC dataset names in the events are storage UUIDs; the readable
    catalog.schema.table lives in the symlinks facet. Build the map."""
    out = {}
    for e in events:
        for side in ("inputs", "outputs"):
            for ds in e.get(side) or []:
                sym = (ds.get("facets") or {}).get("symlinks")
                if sym:
                    for ident in sym.get("identifiers") or []:
                        out[ds["name"]] = ident.get("name")
    return out


def edges(events, names, table):
    """All (outputField, inputField, type, subtype, description) tuples emitted
    for `table`, unioned across every event that carries a columnLineage facet
    for it. Also returns the dataset-level dependency edges separately."""
    field_edges, dataset_edges = set(), set()
    facet_seen = False
    for e in events:
        for ds in e.get("outputs") or []:
            if names.get(ds["name"]) != table:
                continue
            cl = (ds.get("facets") or {}).get("columnLineage")
            if not cl:
                continue
            facet_seen = True
            for fname, fdesc in (cl.get("fields") or {}).items():
                for ipt in fdesc.get("inputFields") or []:
                    for tr in ipt.get("transformations") or []:
                        field_edges.add((fname, ipt.get("field"), tr.get("type"),
                                         tr.get("subtype"), tr.get("description")))
            for d in cl.get("dataset") or []:
                for tr in d.get("transformations") or []:
                    dataset_edges.add((d.get("field"), tr.get("type"),
                                       tr.get("subtype"), tr.get("description")))
    return facet_seen, field_edges, dataset_edges


FAILURES = []


def check(label, ok, detail):
    print("%-4s %-64s %s" % ("PASS" if ok else "FAIL", label, detail))
    if not ok:
        FAILURES.append(label)


def fanin(label, events, names, table, out_fields, desc, in_fields, want_gb):
    seen, fedges, dedges = edges(events, names, table)
    check(f"{label}: columnLineage facet present", seen, "facet_seen=%s" % seen)
    if not seen:
        return
    pairs = {(o, i, t, s) for (o, i, t, s, _) in fedges}
    want = {(o, i, "INDIRECT", "TRANSFORMATION") for o in out_fields for i in in_fields}
    check(f"{label}: fan-in {len(out_fields)}x{len(in_fields)} INDIRECT/TRANSFORMATION edges",
          pairs == want, "%d edges (expected %d)" % (len(pairs), len(want)))
    descs = {e[4] for e in fedges}
    check(f"{label}: description names the operator",
          all(d == desc for d in descs), "descs=%s" % sorted(descs))
    gb = sorted(e[0] for e in dedges if e[2] == "GROUP_BY")
    if want_gb:
        check(f"{label}: dataset GROUP_BY on {sorted(want_gb)}",
              gb == sorted(want_gb), "dataset edges: %s" % gb)
    else:
        check(f"{label}: no GROUP_BY dataset dep",
              not gb, "unexpected GROUP_BY: %s" % gb)


def main():
    events = load("events_pandas_fanin_true.jsonl")
    names = symlink_names(events)
    print("=== pandas e2e: %d events, %d named datasets ===\n"
          % (len(events), len(names)))

    tables = {n for n in names.values() if n.startswith(DEMO + ".")}
    check("6 output tables written", len(tables) >= 6, "tables: %d" % len(tables))

    # ---- cell 3: control, untyped select --------------------------------
    seen, fedges, dedges = edges(events, names, DEMO + ".pandas_control_ol")
    check("control: facet present", seen, "facet_seen=%s" % seen)
    want = {("customer_id", "customer_id", "DIRECT", "IDENTITY", ""),
            ("name", "name", "DIRECT", "IDENTITY", "")}
    check("control: identity edges only", fedges == want, "got %s" % sorted(fedges))
    check("control: no dataset deps", not dedges, "got %s" % sorted(dedges))

    # ---- cell 4: mapInPandas -------------------------------------------
    fanin("mapInPandas", events, names, DEMO + ".pandas_mapinpandas_ol",
          ("o1", "o2"), "MapInPandas(map_pandas)", ALL_IN, False)

    # ---- cell 5: mapInArrow --------------------------------------------
    fanin("mapInArrow", events, names, DEMO + ".pandas_mapinarrow_ol",
          ("o1", "o2"), "MapInArrow(map_arrow)", ALL_IN, False)

    # ---- cell 6: applyInPandas groupBy(real column) --------------------
    fanin("applyInPandas groupBy(col)", events, names, DEMO + ".pandas_applyinpandas_ol",
          ("o1", "o2"), "FlatMapGroupsInPandas(apply_pandas)", ALL_IN, ("country",))

    # ---- cell 7: applyInPandas expression key (ceil) -------------------
    # GROUP_BY resolves through the Alias in the Project below to customer_id.
    fanin("applyInPandas expr key", events, names, DEMO + ".pandas_applyexpr_ol",
          ("o1", "o2"), "FlatMapGroupsInPandas(apply_pandas)", ALL_IN, ("customer_id",))

    # ---- cell 8: applyInArrow ------------------------------------------
    fanin("applyInArrow", events, names, DEMO + ".pandas_applyinarrow_ol",
          ("o1", "o2"), "FlatMapGroupsInArrow(apply_arrow)", ALL_IN, ("country",))

    print()
    if FAILURES:
        print("=== %d FAILURES ===" % len(FAILURES))
        for f in FAILURES:
            print(" -", f)
        sys.exit(1)
    print("=== ALL CHECKS PASSED ===")


if __name__ == "__main__":
    main()
