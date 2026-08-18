#!/usr/bin/env python3
"""Assert that @chris-twiner's review point on sparkutils/OpenLineage#1 is met
on a real Databricks cluster (kanban t_ddb87c95).

The review point:

    MapElements does however show inputs and output type:
        argumentSchema: StructType,
        outputObjAttr: Attribute,
    so we should be able to provide that information.

"Provide that information" is read here as: the emitted columnLineage
transformation description names the argument type and the output type of the
opaque lambda, instead of naming only the plan node. That is what these checks
measure, against the RunEvent JSON captured from the cluster.

Three further properties are asserted because they bound the change:

  * the edge SET is unchanged - the metadata is descriptive, not a narrowing,
    and above all not a per-field pairing across the boundary;
  * MapPartitions, which Spark gives no argumentSchema, claims only its output
    type rather than fabricating an argument side;
  * MapGroups is untouched and still reports the bare operator name, so the
    scope of the change stays honest.

Run:

    python3 docs/evidence/t_ddb87c95/assert_events.py

Exit 0 = every expectation held on DBR 17.3 / Spark 4.0.0 / Scala 2.13.
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DEMO = "databricks_ws.openlineage_demo"
EVENTS = "events_typeinfo_fanin_true.jsonl"


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
    catalog.schema.table lives in the symlinks facet."""
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
    for `table`, plus the dataset-level dependency edges."""
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
    print("%-4s %-62s %s" % ("PASS" if ok else "FAIL", label, detail))
    if not ok:
        FAILURES.append(label)


on = load(EVENTS)
print("=== events loaded: %d (fan-in ON) ===\n" % len(on))

# ------------------------------------------------------- CONSTRUCT F: case class
# The case Chris's example describes: a case-class encoder on both sides, where
# the type names are meaningful. This is the headline check.
seen, fields, _ = edges(on, DEMO + ".typed_map_caseclass_ol")
descs = sorted({e[4] for e in fields})

check("case-class map: facet emitted with fan-in ON", seen and bool(fields),
      "%d field edges" % len(fields))
check("case-class map: exactly one description for the boundary",
      len(descs) == 1, repr(descs))

desc = descs[0] if descs else ""
check("case-class map: description names MapElements with a type arrow",
      "MapElements(" in desc and "->" in desc, repr(desc))
check("case-class map: ARGUMENT type is named (PersonOl)",
      "PersonOl" in desc, repr(desc))
check("case-class map: OUTPUT type is named (ContactOl)",
      "ContactOl" in desc, repr(desc))
check("case-class map: argument encoder FIELDS are named",
      "customer_id" in desc and "email" in desc, repr(desc))
check("case-class map: every edge INDIRECT/TRANSFORMATION",
      bool(fields) and all(e[2] == "INDIRECT" and e[3] == "TRANSFORMATION"
                           for e in fields),
      sorted({(e[2], e[3]) for e in fields}))

# The edge set must be the full cross product, NOT a per-field pairing. 2 output
# fields x 4 input columns = 8. A name-matching pairing would collapse this
# towards 2 while keeping the identity-looking edges - the exact failure the
# visitor's javadoc argues is worse than over-claiming.
check("case-class map: 2x4 pessimistic fan-in preserved (no pairing)",
      len(fields) == 8, "%d field edges: %s" % (len(fields), sorted(fields)[:3]))
outs = {e[0] for e in fields}
ins = {e[1] for e in fields}
check("case-class map: every output linked to every input",
      len(outs) == 2 and len(ins) == 4 and len(fields) == len(outs) * len(ins),
      "outputs=%s inputs=%s" % (sorted(outs), sorted(ins)))

# ------------------------------------------------------- CONSTRUCT D: tuple map
# Kept from t_511e1153. Honest limitation: tuple encoders have positional field
# names, so the added metadata degrades to Tuple2[_1, _2] - present and correct,
# but of little use to a consumer. Asserted so the limitation is on the record.
seen_t, fields_t, _ = edges(on, DEMO + ".typed_map_ol")
descs_t = sorted({e[4] for e in fields_t})
desc_t = descs_t[0] if descs_t else ""
check("tuple map: description carries type info", "MapElements(" in desc_t and "->" in desc_t,
      repr(desc_t))
check("tuple map: types are POSITIONAL (documented limitation)",
      "Tuple2" in desc_t, repr(desc_t))
check("tuple map: 2x4 fan-in preserved", len(fields_t) == 8,
      "%d field edges" % len(fields_t))

# --------------------------------------------- CONSTRUCT G: mapPartitions
# Spark keeps no argumentSchema on MapPartitions, so the argument side must be
# empty rather than guessed at from the child plan.
seen_p, fields_p, _ = edges(on, DEMO + ".typed_mappartitions_ol")
descs_p = sorted({e[4] for e in fields_p})
desc_p = descs_p[0] if descs_p else ""
check("mapPartitions: facet emitted", seen_p and bool(fields_p),
      "%d field edges" % len(fields_p))
check("mapPartitions: output type named, argument side EMPTY",
      "MapPartitions(-> " in desc_p, repr(desc_p))
check("mapPartitions: no fabricated argument type",
      "MapPartitions(Row" not in desc_p and "MapPartitions(Person" not in desc_p,
      repr(desc_p))

# --------------------------------------------- CONSTRUCT C: mapGroups untouched
# Out of scope for this change: MapGroups has deserializers, not an encoder
# pair, so it keeps the bare operator name. Asserting the UNCHANGED case pins
# the scope.
seen_g, fields_g, ds_g = edges(on, DEMO + ".typed_groupby_ol")
descs_g = sorted({e[4] for e in fields_g if e[4]})
check("mapGroups: still the bare operator name (out of scope)",
      any("MapGroups" in d and "MapGroups(" not in d for d in descs_g),
      repr(descs_g))

# --------------------------------------------- CONTROL: UDF / builtin untouched
# Naming the typed boundary must not have disturbed the UDF description or
# downgraded a builtin sitting next to it.
seen_u, fields_u, _ = edges(on, DEMO + ".udf_and_upper_ol")
udf = [e for e in fields_u if e[0] == "masked_email"]
ctl = [e for e in fields_u if e[0] == "country_upper"]
check("UDF edge still INDIRECT/TRANSFORMATION 'UDF: mask_email_ol'",
      udf == [("masked_email", "email", "INDIRECT", "TRANSFORMATION",
               "UDF: mask_email_ol")], repr(udf))
check("builtin upper() in the same table still DIRECT",
      ctl == [("country_upper", "country", "DIRECT", "TRANSFORMATION", "")],
      repr(ctl))

# --------------------------------------------- PACKAGING / runtime provenance
for e in on:
    pe = ((e.get("run") or {}).get("facets") or {}).get("processing_engine")
    if pe:
        check("events carry Spark 4.0.0 processing engine",
              pe.get("version") == "4.0.0",
              repr({k: pe.get(k) for k in ("name", "version")}))
        break
else:
    check("events carry a processing_engine facet", False, "none found")

print()
if FAILURES:
    print("DIVERGENCES: %d" % len(FAILURES))
    for f in FAILURES:
        print("  -", f)
    sys.exit(1)
print("all expectations held")
