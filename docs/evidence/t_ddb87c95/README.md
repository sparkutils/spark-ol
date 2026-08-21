# Databricks validation: MapElements type metadata in column lineage

Kanban card `t_ddb87c95`. Verifies on a real cluster that @chris-twiner's review
point on [sparkutils/OpenLineage#1](https://github.com/sparkutils/OpenLineage/pull/1)
is met:

> MapElements does however show inputs and output type:
> ```scala
>     argumentSchema: StructType,
>     outputObjAttr: Attribute,
> ```
> so we should be able to provide that information. Otherwise this is looking
> great, so I'll merge this when the target branch is amended and the rest can
> go in further changes.

Follow-up to `t_511e1153`, which established the harness. That card's own body
opens by noting every typed-lineage change until then had been verified *only*
against JUnit-built optimized plans; this card exists so the same is not true of
this change.

## Amendment (after review)

The strings quoted throughout this file are verbatim from the cluster run and
are left as-is. They record an earlier revision of the change, in which the
description also listed the encoder's field names, e.g.
`MapElements(PersonOl[customer_id, name, email, country] -> ContactOl)`.

Review of this evidence showed that field list to be redundant: for the
case-class row (F) the names it prints are exactly the set the facet's own
`inputFields` already enumerates on the same edge, and the description is
repeated on all `N x M` edges, so it multiplied a duplicate payload by the edge
count. It was therefore removed, along with its truncation cap. The shipped
description names types only:

```
typed operation: MapElements(PersonOl -> ContactOl), DeserializeToObject
```

Consequences for this document:

- Rows F, D and the verdict block quote the pre-amendment string. The operator
  names, both type names, the edge counts and every `INDIRECT/TRANSFORMATION`
  classification are unaffected — only the bracketed field list is gone.
- Limitation 1 (tuple encoders give positional names) is resolved by removal
  rather than accepted: `Tuple4[_1, _2, _3, _4]` no longer appears anywhere,
  since the field list it complained about is not emitted.
- The `MAX_DESCRIBED_FIELDS` note under "What is still not proven" is void; the
  constant no longer exists. Description size is now constant in schema width,
  which the JUnit suite asserts directly by comparing a 3-column and a 40-column
  encoder.
- **Not re-run on a cluster.** The 565-test JVM suite passes against the
  amended code, but no DBR run was made after the field list was dropped. The
  type names and edge behaviour are unchanged by construction, so this is a
  low-risk gap, not a verified one.

## Verdict

**Met.** On DBR 17.3 LTS / Spark 4.0.0 / Scala 2.13.16 the emitted
`columnLineage` transformation description now names both sides of the opaque
lambda:

```
typed operation: MapElements(PersonOl[customer_id, name, email, country] -> ContactOl), DeserializeToObject
```

quoted verbatim from `events_typeinfo_fanin_true.jsonl`. Before this change the
same boundary reported only `typed operation: MapElements, DeserializeToObject`
(see `../t_511e1153/events_fanin_true.jsonl`).

The edge set is unchanged: still the full 2 × 4 pessimistic fan-in, no per-field
pairing. Two limitations are recorded below rather than papered over.

## What was run

| | |
|---|---|
| Build | `mvn -P Spark4,scala_2.13.16 clean install -DskipTests -Dgpg.skip=true` → `BUILD SUCCESS` (44.9s) |
| Submodule | `core/OpenLineage` at `24acc5d68` (cherry-pick of `9aaad06ca` onto `7a9ec1e4a`) |
| Artefact | `spark-ol_4.0.0.oss_4.0_2.13-0.0.5-RC28.jar`, **847,828** bytes (was 846,351 before the change) |
| Runtime | DBR **17.3** — Spark **4.0.0**, Scala **2.13.16** (all three read back from the live session) |
| Cluster | single-node `Standard_D4ds_v5`, `SINGLE_USER`, autotermination 20 min, **permanently deleted** after the run |
| Transport | `file` → `/local_disk0/ol_events/events.jsonl`, copied out to a UC Volume |
| Runs | notebook run `SUCCESS` (77s), 28 events; plus a Catalyst probe run `SUCCESS` |
| Config | `typedBoundaryFanInEnabled=true` — the description under test rides on the fan-in edges, which ship dark |

Reproduce the verdict from the committed events:

```bash
python3 docs/evidence/t_ddb87c95/assert_events.py
# 19 checks, all PASS
```

## The deployed jar is the version under test

A stale-jar false positive is the obvious way to fake this result, so it is
excluded directly. `catalyst_probe.txt`, printed on the cluster:

```
visitor codesource = file:/databricks/jars/zzz_spark-ol_4.0.0.oss_4.0_2.13-0.0.5-RC28.jar
visitor has describeTypedOperator = true
visitor has argumentTypeName = true
visitor has objectTypeName = true
```

Those three private methods exist only after this change.

## Chris's premise, checked against DBR's own Catalyst

Databricks ships a forked Catalyst, so `javap` against a vanilla Apache 4.0.0
jar is evidence about Apache, not about DBR. The accessors were therefore read
by reflection off the classes loaded on the cluster (`catalyst_probe.txt`):

```
--- MapElements on DBR
    argumentClass  = java.lang.Class
    argumentSchema = org.apache.spark.sql.types.StructType
    outputObjAttr  = org.apache.spark.sql.catalyst.expressions.Attribute
--- MapPartitions on DBR
    argumentClass  = <ABSENT>
    argumentSchema = <ABSENT>
    outputObjAttr  = org.apache.spark.sql.catalyst.expressions.Attribute
```

So Chris's premise holds on DBR for `MapElements` — and the same probe shows it
does **not** hold for `MapPartitions`, which has no `argumentSchema` at all.

Live values off the optimized plan of a case-class `map`:

```
argumentClass  = $line…$PersonP
argumentSchema = [customer_id, name, email, country]
outputObjAttr  = obj : ObjectType(class $line…$ContactP)
```

The `$line…$iw$iw$` prefixes are the Databricks REPL's class wrappers.
`Class.getSimpleName` strips them, which is why the description reads `PersonOl`
rather than the mangled name — worth knowing, because a `getName`-based
implementation would have emitted the wrapper garbage into the facet.

## Per-construct verdict

Every observation quoted from `events_typeinfo_fanin_true.jsonl` and re-checked
by `assert_events.py`.

| # | Construct | Expected | Observed | |
|---|---|---|---|---|
| F | case-class `map` (`PersonOl` → `ContactOl`) | both types named | `MapElements(PersonOl[customer_id, name, email, country] -> ContactOl)`, 8 × `INDIRECT/TRANSFORMATION` | PASS |
| F′ | same, edge set | unchanged 2 × 4 fan-in, no pairing | 2 outputs × 4 inputs = 8 edges, every output linked to every input | PASS |
| D | tuple `map` (as in `t_511e1153`) | types named, but positional | `MapElements(Tuple4[_1, _2, _3, _4] -> Tuple2)` | PASS-with-caveat |
| G | `mapPartitions` | output type only, argument side empty | `MapPartitions(-> ContactOl)` | PASS |
| C | `groupByKey().mapGroups` | **unchanged** — out of scope | `typed operation: MapGroups, AppendColumns` | PASS |
| A | registered UDF `mask_email_ol` | undisturbed | `INDIRECT/TRANSFORMATION` desc `"UDF: mask_email_ol"` | PASS |
| A′ | `upper(country)`, same table | stays `DIRECT` | `DIRECT/TRANSFORMATION` desc `""` | PASS |

Rows A and A′ are carried over from `t_511e1153` deliberately: they prove that
naming the typed boundary did not disturb the UDF description or downgrade an
adjacent builtin.

## Limitations this run exposed

Both are honest results, not defects introduced here.

1. **Tuple encoders give positional names.** Row D reads
   `Tuple4[_1, _2, _3, _4] -> Tuple2`, which is accurate and nearly useless to a
   consumer. The metadata only earns its keep with case-class or bean encoders.
   This matters because the *previously validated* notebook used tuple encoders
   throughout — the useful case (row F) had to be added here to exercise it at
   all.
2. **`MapGroups`/`AppendColumns` are still bare names.** They carry
   deserializers rather than an encoder pair, so they fall outside this change.
   A typed `groupByKey().mapGroups` therefore still reports only which operators
   were crossed. Row C asserts the unchanged behaviour so the scope stays
   explicit; extending coverage there is a separate piece of work.

## What is still not proven

- Only the `_2.13`/Spark 4.0.0 pairing was exercised. The accessors were checked
  by `javap` on 3.2.4 / 3.5.5 / 4.0.0 / 4.1.2 locally and by reflection on DBR
  17.3, but no other DBR runtime was run.
- `MAX_DESCRIBED_FIELDS` truncation (a >8-column encoder) is covered by the
  JUnit suite, not by this cluster run — the demo table has 4 usable columns.

## Cost hygiene

- Cluster `spark-ol-typeinfo-tddb87c95` **permanently deleted**; `databricks
  clusters list` shows no non-terminated cluster and the id no longer resolves.
- Outputs written to new `*_ol` table names only
  (`typed_map_caseclass_ol`, `typed_mappartitions_ol`, `typed_map_ol`,
  `typed_groupby_ol`, `udf_and_upper_ol`); nothing pre-existing was clobbered.
- Workspace ids and UPNs redacted from the committed JSON, matching
  `t_511e1153`.

## Files

| File | |
|---|---|
| `notebook_mapelements_typeinfo.scala` | the notebook that was run (one construct per cell) |
| `catalyst_probe.txt` | DBR accessor/visitor reflection output, printed on the cluster |
| `events_typeinfo_fanin_true.jsonl` | 28 captured `RunEvent`s, redacted |
| `assert_events.py` | 19 machine-checked assertions over those events |
| `cluster_spec.json` | the cluster that was created (`USER_UPN` redacted) |
| `init_script_spark-ol-dbx.sh` | jar staging; identical to `t_511e1153`'s, incl. the micrometer group |
