#!/usr/bin/env python3
"""Assert the per-construct column-lineage verdict against the captured
Databricks OpenLineage events for kanban t_511e1153.

This is the machine-readable half of the evidence: the verdict table in
README.md is generated from what this script measures, not eyeballed. Run:

    python3 docs/evidence/t_511e1153/assert_events.py

Exit 0 = every expectation held on a real DBR 17.3 / Spark 4.0.0 / Scala 2.13
cluster; non-zero prints the divergences.
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DEMO = "databricks_ws.openlineage_demo"


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


off = load("events_fanin_false.jsonl")
on = load("events_fanin_true.jsonl")

print("=== events loaded: fan-in OFF %d, fan-in ON %d ===\n" % (len(off), len(on)))

# ---------------------------------------------------------------- CONSTRUCT A
# A registered Spark UDF must be INDIRECT/TRANSFORMATION and must carry the
# UDF's registered name in the transformation description, so a consumer can
# tell myUdf(email) from upper(email).
for tag, events in (("fan-in OFF", off), ("fan-in ON", on)):
    seen, fields, _ = edges(events, DEMO + ".udf_and_upper_ol")
    udf = [e for e in fields if e[0] == "masked_email"]
    check("UDF edge is INDIRECT/TRANSFORMATION (%s)" % tag,
          udf == [("masked_email", "email", "INDIRECT", "TRANSFORMATION", "UDF: mask_email_ol")],
          repr(udf))

    # CONTROL, same output table, same event: a builtin must stay DIRECT. If
    # this ever reads INDIRECT the UDF result above proves nothing.
    #
    # Measured detail worth pinning: a builtin's transformation carries an
    # EMPTY description (""), not a missing one. So "has a description" is not
    # the discriminator between a UDF edge and a builtin edge — the type is
    # (INDIRECT vs DIRECT), and the description then names which UDF.
    ctl = [e for e in fields if e[0] == "country_upper"]
    check("upper() in the SAME table stays DIRECT (%s)" % tag,
          ctl == [("country_upper", "country", "DIRECT", "TRANSFORMATION", "")],
          repr(ctl))

# ---------------------------------------------------------------- CONSTRUCT B
# Typed filter(FilterFunction) -> TypedFilter -> INDIRECT/FILTER, shipped ON,
# and it lands as a DATASET-level dependency (a filter constrains rows, not a
# specific output column), which is why datasetLineageEnabled=true matters.
for tag, events in (("fan-in OFF", off), ("fan-in ON", on)):
    seen, fields, dsedges = edges(events, DEMO + ".typed_filter_ol")
    filt = sorted(e for e in dsedges if e[2] == "FILTER")
    check("typed filter -> INDIRECT/FILTER on 4 inputs (%s)" % tag,
          len(filt) == 4 and all(e[1] == "INDIRECT" for e in filt),
          "%d dataset edges: %s" % (len(filt), sorted(e[0] for e in filt)))
    check("typed filter passthrough fields stay DIRECT/IDENTITY (%s)" % tag,
          all(e[2] == "DIRECT" and e[3] == "IDENTITY" for e in fields),
          "%d field edges" % len(fields))

# ---------------------------------------------------------------- CONSTRUCT C
# groupByKey().mapGroups -> MapGroups -> INDIRECT/GROUP_BY, shipped ON.
# BUT: the edge is a dataset-level dependency, and buildDatasetDependencies
# only renders when at least one field already has inputs. Across a typed
# boundary no output field has inputs unless the fan-in supplies them, so with
# the fan-in OFF the whole facet is suppressed. This is what
# MapGroupsColumnLineageTest asserts too (facetFields(ds) isEmpty).
seen_off, _, ds_off = edges(off, DEMO + ".typed_groupby_ol")
check("mapGroups: NO facet at all with fan-in OFF (documented)",
      not seen_off and not ds_off,
      "facet present=%s, dataset edges=%d" % (seen_off, len(ds_off)))

seen_on, f_on, ds_on = edges(on, DEMO + ".typed_groupby_ol")
gb = sorted(e for e in ds_on if e[2] == "GROUP_BY")
check("mapGroups -> INDIRECT/GROUP_BY on 4 inputs with fan-in ON",
      len(gb) == 4 and all(e[1] == "INDIRECT" for e in gb),
      "%d dataset edges: %s" % (len(gb), sorted(e[0] for e in gb)))

# ---------------------------------------------------------------- CONSTRUCT D
# Typed map: nothing with the flag off, pessimistic fan-in with it on.
seen_off, f_off, ds_off = edges(off, DEMO + ".typed_map_ol")
check("typed map: NO columnLineage facet with fan-in OFF",
      not seen_off and not f_off and not ds_off,
      "facet present=%s, field edges=%d" % (seen_off, len(f_off)))

seen_on, f_on, _ = edges(on, DEMO + ".typed_map_ol")
# 2 output fields x 4 input columns = the full pessimistic cross product.
expect_desc = "typed operation: MapElements, DeserializeToObject"
check("typed map: 2x4 pessimistic fan-in with fan-in ON",
      len(f_on) == 8
      and all(e[2] == "INDIRECT" and e[3] == "TRANSFORMATION" for e in f_on)
      and all(e[4] == expect_desc for e in f_on),
      "%d field edges, descriptions=%s"
      % (len(f_on), sorted({e[4] for e in f_on})))

# ---------------------------------------------------------------- CONTROL
# A standalone all-builtin table: every edge DIRECT, no INDIRECT anywhere.
for tag, events in (("fan-in OFF", off), ("fan-in ON", on)):
    seen, fields, dsedges = edges(events, DEMO + ".upper_control_ol")
    check("upper()/concat() control table is entirely DIRECT (%s)" % tag,
          seen and fields and all(e[2] == "DIRECT" for e in fields) and not dsedges,
          "%d field edges, types=%s" % (len(fields), sorted({e[2] for e in fields})))

# ---------------------------------------------------------------- PACKAGING
# The headline packaging claim: our build ran on Scala 2.13 and emitted events.
# If openlineage-spark_2.13's NoClassDefFoundError: scala/Serializable had
# reproduced, the listener would never have initialised and there would be no
# events to parse at all.
for tag, events in (("fan-in OFF", off), ("fan-in ON", on)):
    eng = None
    for e in events:
        pe = ((e.get("run") or {}).get("facets") or {}).get("processing_engine")
        if pe:
            eng = pe
            break
    check("events carry Spark 4.0.0 processing engine (%s)" % tag,
          bool(eng) and eng.get("version") == "4.0.0",
          repr(eng and {k: eng[k] for k in ("name", "version")}))

print()
if FAILURES:
    print("DIVERGENCES: %d" % len(FAILURES))
    for f in FAILURES:
        print("  -", f)
    sys.exit(1)
print("all expectations held")
