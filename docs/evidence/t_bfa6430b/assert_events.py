#!/usr/bin/env python3
"""Assert the per-construct column-lineage verdict against the captured
Databricks OpenLineage events for kanban t_bfa6430b.

The machine-readable half of the evidence: the verdict table in README.md is
generated from what this script measures, not eyeballed. Run:

    python3 docs/evidence/t_bfa6430b/assert_events.py

Exit 0 = every expectation held on a real DBR 17.3 / Spark 4.0.0 / Scala 2.13
cluster; non-zero prints the divergences.

Note on dataset names: UC dataset names in the events are storage UUIDs;
the readable catalog.schema.table lives in the symlinks facet. The helpers
below resolve through that map, same as the t_511e1153 assert script.
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DEMO = "databricks_ws.openlineage_demo"
RAW = DEMO + ".raw_customers"


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


def edges(events, table):
    """All (outputField, inputField, type, subtype, description) tuples emitted
    for `table`, unioned across every event that carries a columnLineage facet
    for it. Also returns the dataset-level dependency edges separately."""
    names = symlink_names(events)
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
    print("%-4s %-58s %s" % ("PASS" if ok else "FAIL", label, detail))
    if not ok:
        FAILURES.append(label)


events = load("events_tws_fanin_true.jsonl")
print("=== events loaded: %d ===\n" % len(events))

INPUT_FIELDS = ("customer_id", "name", "email", "country")
OUTPUT_FIELDS = ("country", "customer_count")

# TWS edges carry the operator description 'TransformWithState((String, Tuple4) -> Tuple2), AppendColumns'
# — the established typed-boundary semantics: operator + child AppendColumns comma-joined.
TWS_DESC = "TransformWithState((String, Tuple4) -> Tuple2), AppendColumns"

# --------------------------------------------------------------- CONSTRUCT TWS-1
# groupByKey(...).transformWithState(processor, TimeMode.None(), OutputMode.Append())
# — the construct that was completely invisible before this ticket.
seen, fields, dsedges = edges(events, DEMO + ".tws_no_init_ol")
check("TWS-1 columnLineage facet present", seen, "facet_seen=%s" % seen)
expected = {(o, i, "INDIRECT", "TRANSFORMATION", TWS_DESC)
            for o in OUTPUT_FIELDS for i in INPUT_FIELDS}
check("TWS-1 fan-in: both outputs from all 4 inputs, INDIRECT/TRANSFORMATION",
      fields == expected,
      "%d edges (expected %d)" % (len(fields), len(expected)))
gb = sorted(e[0] for e in dsedges if e[2] == "GROUP_BY")
check("TWS-1 dataset GROUP_BY on all 4 inputs",
      gb == sorted(INPUT_FIELDS), "dataset edges: %s" % gb)

# --------------------------------------------------------------- CONSTRUCT TWS-2
# Same grouping WITH an initial state (StatefulProcessorWithInitialState) —
# a second AppendColumns branch on the right side of TransformWithState
# (BinaryNode). The grouping edges must still resolve through the LEFT side
# only; initial-state columns are not inputs to the fan-in.
seen, fields, dsedges = edges(events, DEMO + ".tws_init_ol")
check("TWS-2 columnLineage facet present", seen, "facet_seen=%s" % seen)
expected = {(o, i, "INDIRECT", "TRANSFORMATION", TWS_DESC)
            for o in INPUT_FIELDS for i in INPUT_FIELDS}  # placeholder
expected = {(o, i, "INDIRECT", "TRANSFORMATION", TWS_DESC)
            for o in OUTPUT_FIELDS for i in INPUT_FIELDS}
check("TWS-2 fan-in: same shape as TWS-1 (left-side resolution only)",
      fields == expected,
      "%d edges (expected %d)" % (len(fields), len(expected)))
gb = sorted(e[0] for e in dsedges if e[2] == "GROUP_BY")
check("TWS-2 dataset GROUP_BY on all 4 inputs",
      gb == sorted(INPUT_FIELDS), "dataset edges: %s" % gb)

# --------------------------------------------------------------- CONTROL mapGroups
# The v0 API over the SAME source and key: same groupByKey shape that was
# ALREADY covered. Same source, same key lambda, same output shape — if the
# TWS-1 edge shows the same structure as this control, the new visitor
# matches the established semantics.
seen, fields, dsedges = edges(events, DEMO + ".typed_groupby_tws_ol")
check("mapGroups columnLineage facet present", seen, "facet_seen=%s" % seen)
MG_DESC = "MapGroups((String, Tuple4) -> Tuple2), AppendColumns"
expected = {(o, i, "INDIRECT", "TRANSFORMATION", MG_DESC)
            for o in OUTPUT_FIELDS for i in INPUT_FIELDS}
check("mapGroups fan-in: same semantics, MapGroups description",
      fields == expected,
      "%d edges (expected %d)" % (len(fields), len(expected)))
gb = sorted(e[0] for e in dsedges if e[2] == "GROUP_BY")
check("mapGroups dataset GROUP_BY on all 4 inputs",
      gb == sorted(INPUT_FIELDS), "dataset edges: %s" % gb)

# --------------------------------------------------------------- CONTROL untyped
# groupBy().agg() — the equivalent UNTYPED grouping, which AggregateV2Visitor
# has always covered. Pins that the TWS GROUP_BY signal is a typed-counterpart
# of an established signal, not a novel shape.
seen, fields, dsedges = edges(events, DEMO + ".untyped_groupby_tws_ol")
check("untyped groupBy columnLineage facet present", seen, "facet_seen=%s" % seen)
check("untyped groupBy: country DIRECT/IDENTITY",
      fields == {("country", "country", "DIRECT", "IDENTITY", "")},
      repr(fields))
gb = sorted(e[0] for e in dsedges if e[2] == "GROUP_BY")
check("untyped groupBy dataset GROUP_BY on country",
      gb == ["country"], "dataset edges: %s" % gb)

print()
if FAILURES:
    print("FAILURES (%d): %s" % (len(FAILURES), FAILURES))
    sys.exit(1)
print("ALL CHECKS PASSED")