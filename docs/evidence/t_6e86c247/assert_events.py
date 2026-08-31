#!/usr/bin/env python3
"""Assert that the simplified typed-boundary description holds on a real
Databricks cluster (kanban t_6e86c247).

The change: TypedBoundaryFanInVisitor renders the fan-in edge description as the
minimal `MapElements(<in> -> <out>)` form — no `typed operation: ` prose prefix,
no `DeserializeToObject` terminal node, no accessor-name suffix. The description
is encoder-flavour-independent; the fan-in edge set (the actual lineage) is
unchanged.

Three properties are asserted because they bound the change:

  * the bean-typed map renders `MapElements(PersonBean -> PersonBean)` — the
    minimal form, with no prefix, no terminal node, no accessor suffix;
  * the case-class map renders the same minimal shape (`MapElements(PersonOl ->
    ContactOl)`), proving the description does not depend on encoder flavour;
  * the fan-in edge set is unchanged (2x2 for the bean, 2x4 for the case class),
    every edge INDIRECT/TRANSFORMATION.

Run:

    python3 docs/evidence/t_6e86c247/assert_events.py

Exit 0 = every expectation held on DBR 17.3 / Spark 4.0.0 / Scala 2.13.
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DEMO = "databricks_ws.openlineage_demo"
EVENTS = "events_desc_simplified_true.jsonl"


def load(name):
    events = []
    with open(os.path.join(HERE, name)) as f:
        for line in f:
            line = line.strip()
            if line:
                events.append(json.loads(line))
    return events


def symlink_names(events):
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
    names = symlink_names(events)
    field_edges = set()
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
    return facet_seen, field_edges


FAILURES = []


def check(label, ok, detail):
    print("%-4s %-62s %s" % ("PASS" if ok else "FAIL", label, detail))
    if not ok:
        FAILURES.append(label)


on = load(EVENTS)
print("=== events loaded: %d (fan-in ON) ===\n" % len(on))

# ------------------------------------------------- CONSTRUCT B: bean-typed map
# The headline case: a bean encoder on both sides. The description must be the
# minimal `MapElements(PersonBean -> PersonBean)`.
seen, fields = edges(on, DEMO + ".typed_map_bean_ol")
descs = sorted({e[4] for e in fields})

check("bean map: facet emitted with fan-in ON", seen and bool(fields),
      "%d field edges" % len(fields))
check("bean map: exactly one description for the boundary",
      len(descs) == 1, repr(descs))

desc = descs[0] if descs else ""
check("bean map: description is exactly MapElements(PersonBean -> PersonBean)",
      desc == "MapElements(PersonBean -> PersonBean)", repr(desc))
check("bean map: no prose prefix", "typed operation" not in desc, repr(desc))
check("bean map: no terminal node", "DeserializeToObject" not in desc, repr(desc))
check("bean map: no accessor suffix", "getters" not in desc and "setters" not in desc,
      repr(desc))
check("bean map: every edge INDIRECT/TRANSFORMATION",
      bool(fields) and all(e[2] == "INDIRECT" and e[3] == "TRANSFORMATION"
                           for e in fields),
      sorted({(e[2], e[3]) for e in fields}))

# The edge set must be the full cross product, NOT a per-field pairing. 2 output
# fields x 2 input columns = 4.
check("bean map: 2x2 pessimistic fan-in preserved (no pairing)",
      len(fields) == 4, "%d field edges: %s" % (len(fields), sorted(fields)[:3]))
outs = {e[0] for e in fields}
ins = {e[1] for e in fields}
check("bean map: every output linked to every input",
      len(outs) == 2 and len(ins) == 2 and len(fields) == len(outs) * len(ins),
      "outputs=%s inputs=%s" % (sorted(outs), sorted(ins)))

# --------------------------------------- CONSTRUCT F (control): case-class map
# The description must be the same minimal shape, proving it does not depend on
# encoder flavour.
seen_c, fields_c = edges(on, DEMO + ".typed_map_caseclass_ol")
descs_c = sorted({e[4] for e in fields_c})
desc_c = descs_c[0] if descs_c else ""

check("case-class map: facet emitted", seen_c and bool(fields_c),
      "%d field edges" % len(fields_c))
check("case-class map: description is exactly MapElements(PersonOl -> ContactOl)",
      desc_c == "MapElements(PersonOl -> ContactOl)", repr(desc_c))
check("case-class map: no accessor suffix", "getters" not in desc_c and "setters" not in desc_c,
      repr(desc_c))
check("case-class map: 2x4 fan-in preserved", len(fields_c) == 8,
      "%d field edges" % len(fields_c))

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
