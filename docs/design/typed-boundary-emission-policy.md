# Column lineage across the typed Dataset boundary

Design record for column-level lineage on typed Dataset operations —
`map`, `mapPartitions`, `flatMap`, `filter(T => Boolean)` and
`groupByKey().mapGroups`. Covers what the plan actually exposes, why the
pessimistic fan-in is the chosen emission policy for the opaque boundary, and
the two narrower signals that ship unflagged alongside it.

Measurements were taken against real optimized plans on Spark 4.0.0 /
Scala 2.13 with throwaway probes; accessor signatures were checked with `javap`
against `spark-catalyst_2.13` 3.5.0, 4.0.0 and 4.1.0.

## What the boundary exposes

For `people().as(bean(Person)).map(…)` with schema `name, email, age`:

```
SerializeFromObject   output=[age#27, email#28, name#29]
                      serializer fields: age, email, name   (each refs=[])
MapElements           output=[obj#23]
DeserializeToObject   output=[obj#19]  outputObjAttr=19
                      deserializer=initializejavabean(newInstance(Person),
                        (setAge,assertnotnull(age#5)),
                        (setEmail,invoke(email#4.toString())),
                        (setName,invoke(name#3.toString())))
                      deserializer.refs=[age#5, email#4, name#3]
LogicalRDD            output=[name#3, email#4, age#5]
```

The two sides are **not** symmetric. Every `SerializeFromObject.serializer()`
field has `references() == []`, because each is rooted at
`input[0, <BeanClass>, true]` — the single opaque JVM object:

```
static_invoke(UTF8String.fromString(invoke(knownnotnull(assertnotnull(
  input[0, …Person, true])).getEmail()))) AS email#N
```

So the output side yields names and types only, never attribute linkage. Only
the input side (`DeserializeToObject.deserializer()`) carries real
`AttributeReference`s back to the leaf. The gap is precisely
*output-attr → leaf-attr across the boundary*.

Note also that `AppendColumns` has **no** `groupingAttributes()` accessor —
it exposes `deserializer()`, `serializer()`, `newColumns()`, `output()`. The
grouping attributes live on `MapGroups`.

### Classpath

There is exactly one column-lineage `VisitorFactory`, at
`integration/spark/spark3/src/main/java/…/plan/column/VisitorFactory.java`, and
`core/pom.xml` adds `spark3/src/main/java` **unconditionally** — not via
`${openLineageVersionSource}`. `spark40/src/main` contains no `*column*` files.
So a registration in the `spark3` factory takes effect on every profile that
builds `core`, and `core` is only in the reactor for `Spark4`/`Spark41`
(`help:evaluate -Dexpression=project.modules`: `Spark350 → [api]`,
`Spark4 → [api, core]`).

## Emission policy for the opaque boundary: pessimistic fan-in, off by default

Three options were considered: stay silent (1), fan in every output from every
input marked opaque (2), or match the two encoder schemas by field name and
position (3). Option 2 is implemented, gated off; option 3 is rejected.

### Why option 3 was rejected

Name-match rates look encouraging at first:

| Case | serializer out | deserializer refs in | name-matched |
|---|---|---|---|
| identity `Person→Person` | `[age, email, name]` | `[age, email, name]` | **3/3** |
| field-swapping `Person→Person` | `[age, email, name]` | `[age, email, name]` | **3/3** |
| type-changing `Person→Contact` | `[email, label]` | `[age, email, name]` | 1/2 |
| `Person→String` | `[value]` | `[age, email, name]` | **0/1** |

Row two is the problem. An identity map and a map that swaps two fields
(`out.setName(p.getEmail()); out.setEmail(p.getName())`) were compared directly:

```
identity  out=[age, email, name] in=[age, email, name]
swapped   out=[age, email, name] in=[age, email, name]
boundary structure identical?          true
option3 output identical?              true
serializer trees equal (mod exprIds)?  true
```

The serializer trees are byte-identical after normalising exprId numbering,
because in both cases the tree is just "call `getName()`/`getEmail()` on the
opaque object". The reassignment happens in the lambda's bytecode, which is not
in the plan.

So option 3 cannot distinguish a faithful pass-through from a field swap **even
in principle** from plan information. It is not occasionally wrong — it emits
the identical claim in both cases, so its confidence is uncorrelated with its
correctness. A wrong `email <- email` edge is worse than a missing one once
PII/compliance tooling acts on it.

Positional matching does not rescue it: `deserializer().references()` is an
unordered `AttributeSet`, and on a 12-column table it iterates
`[c0, c1, c10, c11, c2, …]` — lexicographic, not schema order. Any positional
fallback must use `DeserializeToObject.child().output()`, which *is* in schema
order; using `references()` silently mis-pairs columns on any table with ≥10
similarly-named fields.

Option 2's failure mode is honest by comparison: it over-claims breadth (for an
opaque lambda, "every output may derive from every input" is literally true)
while never asserting a specific false pairing. Silence (option 1) leaves the
observed pathology in place — a `Project` above the hop emits an edge that
dead-ends, so the facet is empty with no indication why.

### Cost, and why it ships dark with a width cap

Growth is exactly N×M:

| Case | option 2 | option 3 |
|---|---|---|
| identity `Person→Person` (3 cols) | 9 | 3 |
| `Person→String` | 3 | 3 (all fallback) |
| Row-encoder identity map, **60 cols** | **3600** | 60 |

100 edges at width 10, 2 500 at 50, 10 000 at 100, 250 000 at 500. Since
`ColumnLevelLineageBuilder` returns **empty** column lineage past
`RETURNED_INPUT_FIELD_LIMIT` (100 000), a single 320-column identity map
(320² = 102 400) would turn "too much lineage" into "no lineage" for every
field of the dataset.

Hence two conditions on option 2, both part of the design rather than
follow-ups:

| property | default |
|---|---|
| `spark.openlineage.columnLineage.typedBoundaryFanInEnabled` | `false` |
| `spark.openlineage.columnLineage.typedBoundaryFanInMaxEdges` | `10000` |

The cap is checked as `outputCount * inputCount > maxEdges` **before** any edge
is added, and above it the visitor emits **nothing** rather than a fan-in
destined to be discarded. 10 000 is a 100×100 boundary, an order of magnitude
below the width at which the builder's own limit would drop the whole facet.

### Shape of the implementation

One visitor, `TypedBoundaryFanInVisitor`, scoped at the **boundary** rather than
per typed node, because the defect is precisely output-attr → leaf-attr across
`Serialize`/`Deserialize` and the surrounding traversal already works:

- `isDefinedAt` matches `SerializeFromObject` only. From there it walks down
  through the opaque object operators (`MapElements`, `MapPartitions`,
  `MapGroups`, `AppendColumns`) and stops at `DeserializeToObject`, the only
  node whose expressions reference real attributes of the relation below.
  Stopping there leaves a nested typed hop to its own `SerializeFromObject`
  rather than absorbing it.
- Inputs come from `deserializer().references()` (plus `keyDeserializer()` and
  `valueDeserializer()` on `MapGroups`, and `AppendColumns.deserializer()`).
  Outputs are `SerializeFromObject.output()` — usable only as attributes to
  attach edges *to*, per the asymmetry above.
- Every output × every input is emitted as `INDIRECT`/`TRANSFORMATION`,
  description `typed operation: <nodes>`, `masking=false`.
  `TransformationInfo.merge` lets the indirect edge win over the
  `DIRECT/IDENTITY` edge a `Project` above the hop contributes, so no field is
  advertised as a traceable identity across an opaque lambda.
- Self-edges (`output.equals(input)`) are skipped, matching
  `ExpressionTraverser`'s leaf handling.

`flatMap` needs no handling of its own — it lowers to `MapPartitions`.
`ColumnLevelLineageBuilder.getContext()` was added (the field already existed)
because `OperatorVisitor.apply` receives only the builder and the flag has to be
readable from there.

`CoGroup` is deliberately not handled: two grouping sides, no `AppendColumns` of
the shape these visitors resolve.

## `TypedFilter` → INDIRECT/FILTER, unflagged

`TypedFilter` is deliberately **not** matched by the fan-in visitor: it does not
cross the boundary, and it passes its child's attributes through unchanged,
sharing their exprIds, so identity is already inferred edge-free:

```
TypedFilter  output=[name#3, email#4, age#5]
             argumentSchema=struct<age:int,email:string,name:string>
             deserializer.refs=[age#5, email#4, name#3]
             child.output=[name#3, email#4, age#5]
             output exprIds == child output exprIds?  true
```

Its actual defect is the missing row-restriction signal, which is structurally
provable and therefore independent of the fan-in choice — so
`TypedFilterVisitor` ships **unflagged**. It takes the existing `FilterVisitor`
shape (mint a synthetic `ExprId`, `addDatasetDependency`, hand the expression to
`ExpressionTraverser` with `TransformationInfo.indirect(FILTER)`) except that the
expression handed over is `TypedFilter.deserializer()` rather than
`Filter.condition()`, the typed predicate being opaque bytecode.

Measured for `source().filter((FilterFunction<Row>) row -> row.getInt(0) > 1)`
over `a:int, b:int, s:string`:

```
edges (3):  #9<-a INDIRECT/FILTER/mask=false
            #9<-b INDIRECT/FILTER/mask=false
            #9<-s INDIRECT/FILTER/mask=false
```

The identity fields remain inferred edge-free, so these edges **add** the
restriction signal rather than replacing the pass-through.

### The edge set is broader than the untyped path

`deserializer().references()` is the whole deserialized schema, not the columns
the predicate read — the deserializer materialises the entire object before the
lambda runs, and the field access lives in bytecode:

| | edge set | exact? |
|---|---|---|
| typed `filter(row -> row.getInt(0) > 1)` | `a`, `b`, `s` | no — 3 columns for a 1-column predicate |
| untyped `filter(col("a").gt(1))` | `a` | yes |

This is not parity with `filter(Column)`. What makes it acceptable, and it is
asserted as a test rather than claimed, is that the typed edge set is a strict
**superset** of the untyped one: over-broad, but every emitted edge names a
column the deserializer genuinely read, and no specific pairing is ever false.
Narrowing it would require bytecode analysis.

No width cap is applied in `TypedFilterVisitor`: capping here would silently
drop the filter signal on exactly the wide tables where it is most useful. The
width behaviour is pinned by test instead, so a future cap decision is a visible
diff.

## `MapGroups` → INDIRECT/GROUP_BY, unflagged

Same reasoning as `TypedFilter` — that a grouping happened, and that it consumed
the columns the key deserializer materialised, are both structurally provable
from the plan, exactly as for `AggregateVisitor` on the untyped path. Gating a
faithful signal behind the flag that exists to contain an unfaithful one would
leave typed groupings invisible to everyone who has not opted into an unrelated
pessimistic behaviour.

### The grouping key must be resolved through `AppendColumns`

Measured on `SerializeFromObject > MapGroups > AppendColumns > LogicalRDD` over
an `(a, b, s)` source:

| accessor | value |
| --- | --- |
| `MapGroups.groupingAttributes()` | `[value#10]` |
| `MapGroups.dataAttributes()` | `[a#3, b#4, s#5]` |
| `MapGroups.keyDeserializer().references()` | `{value#10}` |
| `AppendColumns.newColumns()` | `[value#10]` |
| `AppendColumns.serializer()` | `[invoke(input[0, java.lang.Integer, true].intValue()) AS value#10]` |
| `AppendColumns.deserializer().references()` | `{a#3, b#4, s#5}` |

`groupingAttributes()` does **not** name a source column — it names the
synthetic key column `AppendColumns` appended. Traversing it directly emits an
edge against `value#10`, which nothing links to the relation, so the facet stays
empty. Same asymmetry as above: the serialize side has no `references()`; only
the deserializer carries real `AttributeReference`s.

So `TypedGroupByVisitor` matches each grouping attribute back to the
`AppendColumns` whose `newColumns` contain that **exprId** and traverses that
node's `deserializer()`. Matching on exprId rather than name or position is what
makes the descent safe — a nested typed grouping deeper in the plan has
different exprIds.

Breadth is the deserialized schema, not the key lambda's reads, for the same
reason as `TypedFilter`: for `groupByKey(row -> row.getInt(0) % 2)` over
`(a, b, s)` the edge set is all three columns.

## RETURNED_INPUT_FIELD_LIMIT did not see dataset-level dependencies

Uncovered while measuring `TypedFilterVisitor`'s width behaviour, and fixed
alongside it. A `TypedFilter` contributes **W** dataset-level dependencies (one
per deserialized column), and `buildFields` replicates every dataset dependency
onto **every** output field, so the rendered facet carries `W × (W + 1)` entries
where the untyped path carries `W × 2`:

| width | typed builder edges | typed facet entries | untyped builder edges | untyped facet entries |
|---|---|---|---|---|
| 12 | 12 | **156** | 2 | 24 |
| 60 | 60 | **3 660** | 2 | 120 |
| 350 | 350 | **122 850** | 2 | 700 |

At width 350 the facet was **not** empty: 122 850 entries were returned against
a 100 000 limit. `fieldsDependencies` was summed from the per-field
`getInputsUsedFor` lists only, and `datasetDependencyInputs` was merged in
afterwards inside `facetInputFields` — so dataset-level dependencies escaped the
limit check entirely and passed through however many there were.

The fix adds the dataset-level contribution to the per-field sum *before*
testing the limit, charged at its true rendered cost:

```
renderedInputFields = fieldsDependencies
                    + datasetDependencyInputs.size() × collected.size()
```

The multiplication is the substance of it. A dataset-level dependency is
replicated onto every emitted output field, so its rendered cost is
(dependencies × fields), not (dependencies) — a naive
`+= datasetDependencies.size()` would still let width 350 through, since 350 is
nowhere near 100 000. The warn message names both contributions and the
replication factor, so an operator seeing an empty facet can tell which side
caused it.

Observable change (Spark 4.0.0/2.13.16 and 4.1.2/2.13.18):

| shape | rendered | before | after |
|---|---|---|---|
| typed filter, width 12 | 156 | returned | returned (unchanged) |
| typed filter, width 350 | 122 850 | **returned** | **empty** |
| untyped filter (1 predicate col), width 350 | 700 | returned | returned (unchanged) |
| untyped filter, predicate over all 350 cols | 122 850 | **returned** | **empty** |

The last row matters: this is a behavioural change to the **untyped, shipped**
path, not a typed-path-only change. It is pre-existing in every
dataset-dependency emitter (`FilterVisitor`, `SortVisitor`, `JoinVisitor`,
`AggregateVisitor`) — what the typed path changes is the magnitude, since those
contribute O(predicate columns) where `TypedFilterVisitor` contributes O(table
width) by construction. Real predicates read few columns, which is why it went
unnoticed; it is nonetheless reachable, and is covered by test rather than left
to be discovered in production.

Two mechanisms worth recording, both of which defeat the obvious test shape:

- The limit is counted in `TransformedInput` entries — the unit the pre-existing
  check already used — which is *not* the emitted `InputField` count.
  `facetInputFields` groups by `Input`, so a field's own identity and a
  dataset-level dependency naming the same column collapse into one `InputField`
  carrying two transformations: width W renders `W × (W + 1)` transformations
  across `W × W` input fields. Both are asserted side by side in
  `DatasetDependencyReturnedFieldLimitTest`.
- `datasetDependencyInputs()` resolves each registered exprId **transitively**
  through `getInputsUsedFor`. Registering an output as its own dataset dependency
  contributes not 1 but that output's entire resolved input set, so the
  per-field and dataset-level sides move together. A test that needs the two
  genuinely separate must register dataset dependencies on exprIds that are not
  outputs.

## Registry and cross-version

Three typed visitors, disjoint by plan node — `SerializeFromObject`,
`TypedFilter`, `MapGroups` — which is what lets them coexist without
double-claiming one. `DeserializeToObject` and `SerializeFromObject` need no
visitors of their own; they are the boundary the others read across. The
registry is pinned in one place, `ColumnLineageVisitorRegistryTest`.

`javap` on catalyst 3.5.0, 4.0.0 and 4.1.0: `DeserializeToObject`,
`SerializeFromObject`, `MapElements`, `MapPartitions`, `MapGroups`,
`AppendColumns` and `TypedFilter` expose identical accessors in all three, so no
shim is needed. Built and tested green under `Spark4`/`scala_2.13.16` and
`Spark41`/`scala_2.13.18`; `Spark350` builds `api` only.
