#!/usr/bin/env python3
"""Assert the per-construct column-lineage verdict against the captured
Databricks OpenLineage events for the comprehensive e2e run.

The machine-readable half of the evidence: the verdict table in README.md is
generated from what this script measures, not eyeballed. Run:

    python3 docs/evidence/t_comprehensive_e2e/assert_events.py

Exit 0 = every expectation held on a real DBR 17.3 / Spark 4.0.0 / Scala 2.13
cluster; non-zero prints the divergences.

Note on dataset names: UC dataset names in the events are storage UUIDs; the
readable catalog.schema.table lives in the symlinks facet. The helpers below
resolve through that map, same as the t_511e1153/t_bfa6430b assert scripts.
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DEMO = "databricks_ws.openlineage_demo"
RAW = DEMO + ".raw_customers"

ALL_IN = ("customer_id", "email", "name", "country")     # 4-column cells
TRIPLE_IN = ("customer_id", "name", "country")            # cells selecting 3 cols


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
        check(f"{label}: dataset GROUP_BY on all {len(in_fields)} input columns",
              gb == sorted(in_fields), "dataset edges: %s" % gb)
    else:
        check(f"{label}: no GROUP_BY dataset dep",
              not gb, "unexpected GROUP_BY: %s" % gb)


def main():
    events = load("events_comprehensive_fanin_true.jsonl")
    names = symlink_names(events)
    print("=== comprehensive e2e: %d events, %d named datasets ===\n"
          % (len(events), len(names)))

    tables = {n for n in names.values() if n.startswith(DEMO + ".")}
    check("14 output tables written", len(tables) >= 14, "tables: %d" % len(tables))

    # ---- cell 3: control, untyped select --------------------------------
    seen, fedges, dedges = edges(events, names, DEMO + ".comprehensive_control_ol")
    check("control: facet present", seen, "facet_seen=%s" % seen)
    want = {("customer_id", "customer_id", "DIRECT", "IDENTITY", ""),
            ("name", "name", "DIRECT", "IDENTITY", "")}
    check("control: identity edges only", fedges == want, "got %s" % sorted(fedges))
    check("control: no dataset deps", not dedges, "got %s" % sorted(dedges))

    # ---- cell 4: typed map -------------------------------------------------
    fanin("typed map", events, names, DEMO + ".comprehensive_map_ol",
          ("country", "customer_id"), "MapElements(Tuple4 -> Tuple2)", ALL_IN, False)

    # ---- cell 5: map over untyped filter below (descent) --------------------
    fanin("map over untyped filter (descent)", events, names, DEMO + ".comprehensive_map_filter_ol",
          ("country", "customer_id"), "MapElements(Tuple4 -> Tuple2)", ALL_IN, False)

    # ---- cell 6: map + untyped filter above (descent, other direction) -------
    fanin("map + filter above (descent)", events, names, DEMO + ".comprehensive_map_filter_above_ol",
          ("country", "customer_id"), "MapElements(Tuple3 -> Tuple2)", TRIPLE_IN, False)

    # ---- cell 7: mapPartitions ------------------------------------------------
    fanin("mapPartitions", events, names, DEMO + ".comprehensive_mappartitions_ol",
          ("country", "customer_id"), "MapPartitions(-> Tuple2)", TRIPLE_IN, False)

    # ---- cell 8: chained map -> mapPartitions -----------------------------------
    fanin("chained map->mapPartitions", events, names, DEMO + ".comprehensive_chained_ol",
          ("country", "customer_id"),
          "MapPartitions(-> Tuple2), MapElements(Tuple3 -> Tuple3), sameType", TRIPLE_IN, False)

    # ---- cell 9: groupByKey().mapGroups -----------------------------------------
    fanin("groupByKey().mapGroups", events, names, DEMO + ".comprehensive_mapgroups_ol",
          ("country", "customer_count"),
          "MapGroups((String, Tuple4) -> Tuple2), AppendColumns", ALL_IN, True)

    # ---- cell 10: map then groupByKey().mapGroups (AppendColumnsWithObject) ------
    fanin("map->groupByKey().mapGroups", events, names, DEMO + ".comprehensive_map_then_group_ol",
          ("country", "customer_count"),
          "MapGroups((String, Tuple4) -> Tuple2), MapElements(Tuple4 -> Tuple4), sameType",
          ALL_IN, True)

    # ---- cell 11: flatMapGroupsWithState (v0 stateful, batch) ---------------------
    # NOTE: the notebook writes `comprehensive_fmGws_ol` (capital G) — keep in sync.
    fanin("flatMapGroupsWithState", events, names, DEMO + ".comprehensive_fmGws_ol",
          ("country", "customer_count"),
          "FlatMapGroupsWithState((String, Tuple4) -> Tuple2), AppendColumns", ALL_IN, True)

    # ---- cell 12: transformWithState no init ---------------------------------------
    fanin("transformWithState", events, names, DEMO + ".comprehensive_tws_no_init_ol",
          ("country", "customer_count"),
          "TransformWithState((String, Tuple4) -> Tuple2), AppendColumns", ALL_IN, True)

    # ---- cell 13: transformWithState with initial state ------------------------------
    fanin("transformWithState+init", events, names, DEMO + ".comprehensive_tws_init_ol",
          ("country", "customer_count"),
          "TransformWithState((String, Tuple4) -> Tuple2), AppendColumns", ALL_IN, True)

    # ---- cell 14: typed filter ------------------------------------------------------
    seen, fedges, dedges = edges(events, names, DEMO + ".comprehensive_typed_filter_ol")
    check("typed filter: facet present", seen, "facet_seen=%s" % seen)
    if seen:
        want = {(o, o, "DIRECT", "IDENTITY", "") for o in ALL_IN}
        check("typed filter: identity fields preserved", fedges == want, "got %s" % sorted(fedges))
        flt = sorted(e[0] for e in dedges if e[2] == "FILTER")
        check("typed filter: dataset FILTER on all 4 deserialized columns",
              flt == sorted(ALL_IN), "FILTER deps: %s" % flt)

    # ---- cell 15: registered UDF ------------------------------------------------------
    seen, fedges, dedges = edges(events, names, DEMO + ".comprehensive_udf_ol")
    check("UDF: facet present", seen, "facet_seen=%s" % seen)
    if seen:
        want = {("customer_id", "customer_id", "DIRECT", "IDENTITY", ""),
                ("shouty", "name", "INDIRECT", "TRANSFORMATION", "UDF: echoUpper")}
        check("UDF: identity + INDIRECT 'UDF: echoUpper'", fedges == want, "got %s" % sorted(fedges))

    # ---- cell 16: compose (map + filter + UDF) -------------------------------------------
    # FINDING: a UDF applied *on top of* a typed boundary has its `UDF: name`
    # marker absorbed by TransformationInfo.merge (rule: an INDIRECT edge wins
    # over the DIRECT identity edge). The fan-in description `MapElements(...)`
    # is emitted on BOTH outputs and no separate `UDF: echoUpper` edge survives.
    # This is expected builder semantics, not a lineage bug — the UDF cell
    # (cell 15, UDF over a plain column) still shows `UDF: echoUpper` intact.
    seen, fedges, dedges = edges(events, names, DEMO + ".comprehensive_compose_ol")
    check("compose: facet present", seen, "facet_seen=%s" % seen)
    if seen:
        fan = {(o, i, t, s) for (o, i, t, s, d) in fedges if "MapElements" in d}
        want = {(o, i, "INDIRECT", "TRANSFORMATION")
                for o in ("country", "shouty") for i in ALL_IN}
        check("compose: MapElements fan-in on both outputs", fan == want,
              "%d of %d" % (len(fan), len(want)))
        # The UDF marker is absorbed into the fan-in — assert it is NOT present
        # as a separate edge (documents the actual builder behaviour).
        udf_edges = [e for e in fedges if "UDF" in e[4]]
        check("compose: UDF marker absorbed into fan-in (no separate UDF edge)",
              not udf_edges, "unexpected UDF edges: %s" % udf_edges)

    print()
    if FAILURES:
        print("=== %d FAILURES ===" % len(FAILURES))
        for f in FAILURES:
            print(" -", f)
        sys.exit(1)
    print("=== ALL CHECKS PASSED ===")


if __name__ == "__main__":
    main()