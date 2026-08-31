# Databricks validation: simplified typed-boundary description

Kanban card `t_6e86c247`. Verifies on a real cluster that the typed-boundary
fan-in description is the minimal `MapElements(<in> -> <out>)` form — no
`typed operation: ` prose prefix, no `DeserializeToObject` terminal node, no
accessor-name suffix — and that the fan-in edge set (the actual lineage) is
unchanged.

Follow-up to `t_ddb87c95` (which named the encoder types in the description)
and the accessor-name enrichment that was reviewed and dropped as redundant:
the getter/setter names are derivable from the field names the fan-in already
carries, and the one thing they would add — the getter→setter pairing — is
unrecoverable from the plan (it lives in the lambda bytecode).

## Verdict

**Met.** On DBR 17.3 LTS / Spark 4.0.0 / Scala 2.13.16, a bean-typed `map`
emits:

```
MapElements(PersonBean -> PersonBean)
```

and a case-class `map` emits:

```
MapElements(PersonOl -> ContactOl)
```

both quoted verbatim from `events_desc_simplified_true.jsonl`. The description
is encoder-flavour-independent; the edge set is unchanged (2 × 2 for the bean,
2 × 4 for the case class), every edge `INDIRECT/TRANSFORMATION`.

## What was run

| | |
|---|---|
| Build | `mvn -P Spark4,scala_2.13.16 clean install -Dgpg.skip=true` → `BUILD SUCCESS`, 45 tests, 0 failures |
| Submodule | `core/OpenLineage` at `37831e5f8` + uncommitted description simplification |
| Artefact | `spark-ol_4.0.0.oss_4.0_2.13-0.0.5-RC28.jar`, **846,926** bytes |
| Runtime | DBR **17.3** — Spark **4.0.0**, Scala **2.13.16** (read back from the events' `processing_engine` facet) |
| Cluster | single-node `Standard_D4ds_v5`, `SINGLE_USER`, autotermination 20 min |
| Transport | `file` → `/local_disk0/ol_events/events.jsonl`, copied out to a UC Volume |
| Runs | notebook run `SUCCESS` (52s), 20 events |
| Config | `typedBoundaryFanInEnabled=true` — the description under test rides on the fan-in edges, which ship dark |

Reproduce the verdict from the committed events:

```bash
python3 docs/evidence/t_6e86c247/assert_events.py
# 14 checks, all PASS
```

## The deployed jar is the version under test

A stale-jar false positive is the obvious way to fake this result, so it is
excluded by construction: the minimal description can only be produced by the
simplified `description()` (no `DESCRIPTION_PREFIX`, no `DeserializeToObject`
node name). A stale jar would emit the old `typed operation: MapElements(...),
DeserializeToObject` form. The events carry the minimal form, so the deployed
jar is the version under test.

## Per-construct verdict

Every observation quoted from `events_desc_simplified_true.jsonl` and
re-checked by `assert_events.py`.

| # | Construct | Expected | Observed | |
|---|---|---|---|---|
| B | bean `map` (`PersonBean` → `PersonBean`) | minimal description | `MapElements(PersonBean -> PersonBean)`, 4 × `INDIRECT/TRANSFORMATION` | PASS |
| B′ | same, edge set | unchanged 2 × 2 fan-in, no pairing | 2 outputs × 2 inputs = 4 edges, every output linked to every input | PASS |
| F | case-class `map` (`PersonOl` → `ContactOl`) | same minimal shape | `MapElements(PersonOl -> ContactOl)`, 8 edges | PASS |

## What is still not proven

- Only the `_2.13` / Spark 4.0.0 pairing was exercised, as in the prior cards.
- The `AppendColumns` node name (MapGroups-specific) is not exercised here; it
  is covered by `MapGroupsColumnLineageTest` in the JUnit suite.

## Cost hygiene

- Cluster `spark-ol-accessor-t6e86c247` (`0824-134436-2l063fs7`) **permanently
  deleted** after the run.
- Outputs written to new `*_ol` table names only (`typed_map_bean_ol`,
  `typed_map_bean_src_ol`); the case-class control reuses `typed_map_caseclass_ol`
  from `t_ddb87c95` (overwritten, same shape).
- Workspace UPN and org id redacted from the committed JSON, matching
  `t_511e1153` / `t_ddb87c95`.

## Files

| File | |
|---|---|
| `notebook_accessor_names.scala` | the notebook that was run (one construct per cell) |
| `events_desc_simplified_true.jsonl` | 20 captured `RunEvent`s, redacted |
| `assert_events.py` | 14 machine-checked assertions over those events |
| `cluster_spec.json` | the cluster that was created (`USER_UPN` redacted) |
