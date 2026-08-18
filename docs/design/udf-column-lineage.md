# Column lineage for Spark UDFs

Design record for column-level lineage on user-defined functions, and the
cross-version/module constraints that apply to it and to the typed-operation
work in [typed-boundary-emission-policy.md](typed-boundary-emission-policy.md).

## The defect: edge quality, not edge absence

UDF arguments already reach the output. `ExpressionTraverser`'s generic
`children()` fallback (line 115-121) walks them, so a `ScalaUDF` over two
columns emits a fan-in edge from each — but they arrive as
`DIRECT`/`TRANSFORMATION`, **byte-identical** to plain `a + b`. Measured against
unmodified production code on Spark 4.0.0 / Scala 2.13:

| Operation | Edges | TransformationInfo | Plan-node chain |
|---|---|---|---|
| `select(col("a"))` — control | 1 | `DIRECT/IDENTITY/false` | `Project>LogicalRDD` |
| `select(col("a") + col("b"))` — control | 2 | `DIRECT/TRANSFORMATION/false` ×2 | `Project>LogicalRDD` |
| `select(sha1(col("s")))` — masking control | 1 | `DIRECT/TRANSFORMATION/**true**` | `Project>LogicalRDD` |
| `ScalaUDF`, 1 arg | 1 | `DIRECT/TRANSFORMATION/false` | `Project>LogicalRDD` |
| `ScalaUDF`, 2 args | 2 (fan-in) | `DIRECT/TRANSFORMATION/false` ×2 | `Project>LogicalRDD` |
| `ScalaUDF` nested in `ScalaUDF` | 2 (flattened) | `DIRECT/TRANSFORMATION/false` ×2 | `Project>LogicalRDD` |
| `PythonUDF`, 2 args | 2 (fan-in) | `DIRECT/TRANSFORMATION/false` ×2 | `Project` |

So a consumer cannot distinguish `upper(name)`, whose semantics Spark knows,
from `myUdf(name)`, whose body is opaque JVM or Python bytecode. A nested UDF is
indistinguishable from a flat one. The edge is *falsely confident* — and a
falsely confident edge is worse than a missing one, because downstream
PII/compliance tooling acts on it.

Two related observations from the same measurements:

- **`isMasking` only matches a fixed allow-list.** A UDF that genuinely
  obfuscates its input is advertised as `mask=false`. The `sha1` control above
  proves `mask=true` is reachable, so the UDF result is a real negative rather
  than a blind spot.
- **`PythonUDF` exposes its arguments via `children()`.** A directly constructed
  `PythonUDF` (no Python runtime needed) with two `AttributeReference` arguments
  reports `children().size() == 2`, and traversing a `Project` over it yields the
  same edges as `ScalaUDF`. So Python UDFs need no special-casing to *get* edges
  — but any opacity marker must cover them, or they would silently keep the old
  typing while Scala UDFs improved.

## Chosen encoding: reuse `INDIRECT`, carry the function name in `description`

A `UserDefinedExpression` argument is recorded as `INDIRECT`/`TRANSFORMATION`,
`description = "UDF: <name>"`, `masking = false`.

`INDIRECT` reads as "the input demonstrably influenced the output, but the nature
of the influence is unknown", which is exactly a UDF's situation. The fan-in is
preserved; only the unwarranted confidence is removed.

### Rejected alternatives

- **A new `Subtypes` enum value (e.g. `UDF`).** `TransformationInfo.Subtypes` is
  a public enum in `client/java`, serialised into the `columnLineage` facet via
  `toInputFieldsTransformations()` and mirrored by the Python client. Adding a
  value forces every consumer's exhaustive `switch`/match to handle it, and buys
  nothing actionable the description does not already carry — public API churn
  disproportionate to the gain. This is the one option with an API-surface
  implication; revisiting it would need coordinated Java and Python client
  changes.
- **Description string alone, leaving the type `DIRECT`.** Zero enum churn, but
  `DIRECT` is precisely the false claim being corrected, so a marker that leaves
  it in place fails to fix the bug. It also has the weakest queryability: a
  consumer filtering on type still sees a UDF as fully traceable.

### Scope: `UserDefinedExpression`, not `ScalaUDF`

Matching the interface covers `PythonUDF`/`PythonUDAF` (via
`PythonFuncExpression`), `ScalaUDAF`, `ApplyFunctionExpression` (DSv2
`ScalarFunction`) and the Hive UDF/UDAF/UDTF wrappers in one visitor. Covering
`ScalaUDF` alone would leave the two families disagreeing for no visible reason,
which is worse than uniform imprecision. Both interfaces are present and
identical in Spark 3.5, 4.0 and 4.1.

### Deliberate non-goals

- **No per-argument → per-output-field precision.** A UDF body is opaque; fan-in
  plus an honest opacity marker is the correct ceiling. Finer mapping requires
  user declaration.
- **`masking` stays `false`.** Whether a UDF obfuscates its input is unknowable;
  claiming `true` would be as wrong as the `DIRECT` edge just removed. Masking of
  a *known* inner expression still propagates — `plus1(sha1(s))` yields
  `mask=true` via `TransformationInfo.merge`.
- **`udfDeterministic` gets no distinct marker.** A non-deterministic UDF is not
  *more* opaque than a deterministic one, and the encoding has one description
  field.
- **Nested UDFs report the outermost function name.** `merge` rule 1 keeps the
  existing `INDIRECT` info, so `plus1(addTwo(a, b))` describes `plus1`. Naming one
  of the two is the ceiling of a single description field; pinned by assertion so
  a future encoding change surfaces as a test failure.

### A latent NPE this widens but does not trigger

`TransformationInfo.merge` dereferences `another` at the masking comparison
(`this.getMasking().equals(another.getMasking())`) on a path where `another` may
be `null` — reachable only via the `Types.INDIRECT` early branch, which returns
`res = this` without the null guard the `else` branch has. `Dependency.merge` can
pass `null`. Making UDFs `INDIRECT` widens the set of callers taking that branch,
so it is worth recording, but it is not triggerable today:
`ColumnLevelLineageBuilder.addDependency` never stores a `null`
`TransformationInfo`, and `findDependentInputs` seeds its search with
`TransformationInfo.identity()`. No speculative guard added — it would be
untestable.

## Cross-version compatibility: no conditional compilation needed

The plan nodes and expressions this work dispatches on are present with
**byte-identical public signatures** across the supported range. Verified with
`javap -public` against the jars in `~/.m2`, diffing the normalised signature
dump of each class across versions (`spark-catalyst_2.13` at 3.5.0, 4.0.0,
4.1.2):

| Class (`org.apache.spark.sql.catalyst.…`) | 3.5.0 | 4.0.0 | 4.1.2 | Drift |
| --- | :---: | :---: | :---: | --- |
| `plans.logical.MapElements` | yes | yes | yes | none |
| `plans.logical.MapPartitions` | yes | yes | yes | none |
| `plans.logical.TypedFilter` | yes | yes | yes | none |
| `plans.logical.SerializeFromObject` | yes | yes | yes | none |
| `plans.logical.DeserializeToObject` | yes | yes | yes | none |
| `plans.logical.AppendColumns` | yes | yes | yes | none |
| `plans.logical.AppendColumnsWithObject` | yes | yes | yes | none |
| `plans.logical.MapGroups` | yes | yes | yes | none |
| `plans.logical.CoGroup` | yes | yes | yes | none |
| `expressions.ScalaUDF` | yes | yes | yes | none |
| `expressions.PythonUDF` | yes | yes | yes | none |
| `expressions.Mask` | yes | yes | yes | none |

The full signature dump for the seven classes actually dispatched on is 289 lines
and identical across all three versions — `diff` reports no differences for
3.5.0→4.0.0 or 4.0.0→4.1.2. Scala 2.12 (`spark-catalyst_2.12:3.5.0`, used by the
3.5 / early-DBR profiles) also carries `MapElements`, `TypedFilter`,
`AppendColumns`, `ScalaUDF` and `Mask`.

So: write the visitors once against the plain types in
`spark3/…/plan/column/`. No shims, no reflection, no per-profile source
directories, and no `<excludes>` entries — the existing `<excludes>` blocks in
`core/pom.xml` exist for Iceberg/Delta/streaming builders with genuine version
problems, and adding entries for these visitors would have to be maintained in
three near-identical profile blocks for no benefit.

Use the reflective soft-guard only for classes actually proven absent somewhere
in the support matrix. The pattern already exists in `ExpressionTraverser`:

```java
private static final List<String> classNames =
    Collections.singletonList("org.apache.spark.sql.catalyst.expressions.Mask");
// matched via expression.getClass().getCanonicalName()
```

with `ReflectionUtils.hasClass(String)` / `hasClasses(String...)` for gating
whole visitors. `Mask` is the only version-variant class in this area, and it is
present in every currently supported runtime — the guard is legacy but worth
keeping, and it is the right tool if a *Databricks* runtime turns out to relocate
a class, since that cannot be caught at compile time.

## The real constraint: module gating

`core` — where all of the column-lineage code compiles — is only listed in the
`<modules>` block of the `Spark4` and `Spark41` profiles. `Spark350` builds `api`
alone, and `api` compiles zero OpenLineage sources (two `ProxyTransport*` classes).

Reactor composition, measured with `help:evaluate -Dexpression=project.modules`:

| Profile combination | Modules | `sparkCompatVersion` | `openLineageVersionSource` | Column lineage built? |
| --- | --- | :---: | --- | :---: |
| `Spark350,scala_2.12.18` | `api` | 3.5 | `spark35` | **no** |
| `Spark4,scala_2.13.16` | `api`, `core` | 4.0 | `spark40` | yes |
| `Spark41,scala_2.13.18` | `api`, `core` | 4.1 | `spark40` | yes |
| `14.3.dbr,Spark350,scala_2.12.18` | `api` | 3.5 | `spark35` | **no** |
| `15.4.dbr,Spark350,scala_2.12.18` | `api` | 3.5 | `spark35` | **no** |
| `16.4.dbr,Spark350,scala_2.12.18` | `api` | 3.5 | `spark35` | **no** |
| `17.3.dbr,Spark4,scala_2.13.16` | `api`, `core` | 4.0 | `spark40` | yes |
| `18.3.dbr,Spark41,scala_2.13.18` | `api`, `core` | 4.1 | `spark40` | yes |

Two consequences, both more significant than any API question:

1. **The `.dbr` profiles declare no `<modules>` of their own.** They must be
   combined with a Spark profile — `-P 17.3.dbr,Spark4,scala_2.13.16`, not
   `-P 17.3.dbr` alone, which yields an empty module list and silently builds
   nothing but the root pom.
2. **UDF and typed-operation lineage does not ship on Spark 3.5 or DBR
   14.3/15.4/16.4**, because `core` is not in those reactors. This is a
   pre-existing property of the build layout, not something this work introduces.
   Extending it there means adding `core` to the `Spark350` module list and
   dealing with the compile fallout — a scoping decision, not something to
   smuggle in via conditional compilation.

### Stale `core/target/classes` is a verification trap

`clean` only visits modules in the active reactor. After a `Spark4` build,
`core/target/classes` still holds its compiled classes; a subsequent `Spark350`
build leaves them untouched, so `find`-based checks appear to show
column-lineage classes present in the 3.5 profile. They are stale artifacts.
Confirmed by moving `core/target/classes` aside and rebuilding
`-P Spark350,scala_2.12.18 clean install`: the directory is **not** recreated.
Compare mtimes against build start time when verifying.

## Build and test evidence

All commands run with `-Dgpg.skip=true` (no local signing key;
`maven-gpg-plugin` otherwise fails `install` with `Exit code: 2`, unrelated to
this work).

| Profile combination | `clean install -DskipTests` |
| --- | --- |
| `Spark4,scala_2.13.16` | BUILD SUCCESS |
| `Spark41,scala_2.13.18` | BUILD SUCCESS |
| `17.3.dbr,Spark4,scala_2.13.16` | BUILD SUCCESS |
| `18.3.dbr,Spark41,scala_2.13.18` | BUILD SUCCESS |
| `Spark350,scala_2.12.18` | BUILD SUCCESS (`core` not in reactor) |
| `14.3.dbr,Spark350,scala_2.12.18` | BUILD SUCCESS |
| `15.4.dbr,Spark350,scala_2.12.18` | BUILD SUCCESS |
| `16.4.dbr,Spark350,scala_2.12.18` | BUILD SUCCESS |

`Spark4` and `Spark41` produce identical class sets (`diff` clean), further
evidence that per-version source trees would be dead weight.

Databricks caveat: the `.dbr` profiles resolve the **OSS** Spark jars — there are
no `*-dbr*` classified Spark artifacts in the local repository. A green `.dbr`
build validates the compile against OSS Spark of the matching version, not
against a real Databricks runtime jar. Runtime-only divergence on DBR remains
untested, which is what the reflective soft-guard is for.

### Regression guard

`TypedOperationCompatibilityTest` asserts that every plan node and UDF expression
these visitors depend on is loadable on the runtime under test, so a future Spark
or DBR bump that removes or relocates one fails the build rather than silently
degrading lineage. That failure mode is the reason it asserts presence rather
than skipping: `ExpressionDependencyCollector#collectFromOperator` filters on
`isDefinedAt`, so a class that quietly vanishes produces **no edges and no
warning**.

```bash
mvn -P Spark4,scala_2.13.16 test -Dtest=TypedOperationCompatibilityTest -pl core -Dgpg.skip=true
```

Negative-controlled: adding a non-existent class name to the list produced
`Tests run: 4, Failures: 1` with
`Expecting empty but was: ["…NegativeControlDoesNotExist"]`, confirming the
assertion is not vacuous. It cannot run on the 3.5 / 14.3 / 15.4 / 16.4 profiles
since `core` is not in those reactors — asserted explicitly by
`sanityCheckRunsOnSpark4OrLater`, which fails loudly if the module layout changes.

## Test-harness constraints

Three traps that will catch anyone extending these tests:

1. **Do not build the fixture with `createDataFrame(List)` or `selectExpr`
   literals.** It folds to a `LocalRelation` and the optimizer evaluates the UDF
   away at plan time — the plan becomes `LocalRelation [out#2]` with no
   `Project`, and traversal correctly reports 0 edges. An artifact of the
   fixture, not integration behaviour, and it silently invalidates the test.
2. **Do not use `spark.range` when asserting fan-in.** Its single `id` column
   makes multi-argument fan-in unobservable: both UDF arguments resolve back to
   the same input field, so a 2-argument UDF appears to emit two identical
   `out<-id` edges. Use a leaf with genuinely distinct columns — these suites use
   `sparkContext.parallelize` → `LogicalRDD`.
3. **File-based sources are unusable in this module's tests.** A `delta-spark`
   artifact on the `core` test classpath is built against a newer Spark and
   `DataSource` resolution dies with `NoClassDefFoundError:
   org/apache/spark/sql/connector/catalog/SupportsV1OverwriteWithSaveAsTable` on
   any `write().parquet(...)`. Hence the RDD leaf — and hence
   `InputFieldsCollector` being stood in for (it resolves a `DatasetIdentifier`
   from the concrete relation type, and an RDD-backed leaf has none) by
   registering every `LeafNode` output attribute against one synthetic dataset,
   which is what the real collector does for a file/table relation.
