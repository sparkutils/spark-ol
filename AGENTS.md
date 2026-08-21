# OpenLineage Spark (spark-ol) — Agent Guide

Maven multi-module project wrapping / extending OpenLineage for Apache Spark
and Databricks runtimes.

## Build system: Maven ONLY

This project builds with **Maven**. Do not add, invoke, or generate Gradle
files (`build.gradle`, `settings.gradle`, `gradlew`, `gradle.properties`) for
this project's own modules.

`core/OpenLineage/` is a **git submodule of our own fork**
(`git@github.com:sparkutils/OpenLineage.git`, branch `temp/Spark4_clean`, with
`upstream` pointing at the real OpenLineage). It is NOT a read-only vendored
snapshot — it is editable, and lineage changes will usually land *there*.

It contains Gradle files because that is upstream's build. We never invoke
them: `core/pom.xml` uses `build-helper-maven-plugin:add-source` to compile the
submodule's Java/Scala sources directly into our Maven reactor. So Gradle stays
unused, but the sources are very much ours to change.

Consequence for any code change: edits under `core/OpenLineage/` are commits in
a *different repository*. Commit them in the submodule, then commit the updated
submodule pointer in the parent. Both repos are push-guarded (see below).

### Module layout

```
pom.xml            spark-ol_root_${dbrCompatVersion}${sparkCompatVersion}_${scalaCompatVersion}
├── api/pom.xml    spark-ol_api_...    transports / client-facing surface
└── core/pom.xml   spark-ol_...        Spark listener, visitors, column lineage
```

`core` is only in the module list for the Spark 4.0 / 4.1 profiles; the
Spark 3.5 profile builds `api` alone. Check the active profile's `<modules>`
block before assuming a module is in the reactor.

### Profiles (must be selected explicitly)

- Scala: `scala_2.12.18`, `scala_2.13.12`, `scala_2.13.16`, `scala_2.13.18`
- Spark: `Spark350`, `Spark4`, `Spark41`
- Databricks: `14.3.dbr`, `15.4.dbr`, `16.4.dbr`, `17.3.dbr`, `18.3.dbr`

Artifact names are profile-derived via `${dbrCompatVersion}`,
`${sparkCompatVersion}`, `${scalaCompatVersion}` — a build with no profile
selected will not resolve the way you expect.

### Commands

**JDK 21 is required.** Lombok 1.18.46 generates the accessors that
`OpenLineageContext` and friends rely on, and its annotation processor does not
run under JDK 17 here: the build fails with ~172 misleading
`cannot find symbol: method getOpenLineage()` errors pointing into
`core/OpenLineage/integration/spark/shared/`. Those errors are a toolchain
symptom, not broken submodule code — set the JDK before blaming the source:

```bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
```

```bash
# always install first — the shim unpack step needs installed artifacts,
# otherwise later phases fail with FILE_NOT_EXIST
mvn -P Spark4,scala_2.13.16 clean install -DskipTests

# single suite (ScalaTest): only -Dsuites="FQCN" actually filters
mvn -P Spark4,scala_2.13.16 test -Dsuites="io.openlineage.client.wrapper.OpenLineageTest"

# single JUnit test
mvn -P Spark4,scala_2.13.16 test -Dtest=ProxyTransportWrapperTest -pl core
```

Run builds as one long-running background command and grep the output for
`Tests:|BUILD|FAILED` — do not poll a live log.

## Publishing: nothing is pushed without approval

**Do not push anything to `origin` (`git@github.com:sparkutils/spark-ol.git`).**
No commits, no branches, no tags, no worktree branches, no PRs. Committing
locally is fine and expected; publishing is a human decision.

A `pre-push` hook enforces this in **both** repositories — the parent at
`.git/hooks/pre-push` and the submodule at
`.git/modules/OpenLineage/hooks/pre-push` (so pushes to
`sparkutils/OpenLineage` are blocked too). Because git worktrees share the
parent repo's hook directory, the guard also covers task worktrees under
`.worktrees/`.

Do **not** work around the guard. Specifically: never set
`SPARK_OL_PUSH_APPROVED=1`, never pass `--no-verify`, never delete or edit the
hook, and never add a second remote to sidestep it. If you believe a push is
needed, stop and ask; the human runs it themselves.

Verify state instead of pushing: `git status -sb` and
`git log --oneline @{u}..HEAD` show what is waiting for approval.

## Conventions

- Java source/target is 1.8 for the shipped surface; `javaLangVersion` 11 is
  used for tooling. Don't raise these casually — Databricks runtimes pin them.
- Line endings in tracked poms are CRLF. Leave them alone; don't reformat.
- `target/` is gitignored. Never commit build output or `.iml` files.
- New Java code goes under the owning module's
  `src/main/java/io/openlineage/...`; tests mirror the package under
  `src/test/java`.

### Scratch and temporary files

Temporary markdown — kanban card bodies, investigation notes, draft docs — plus
throwaway JSON specs and one-off scripts go in `.tmp/` at the repo root:

```
/home/eugene/workspace/spark-ol/.tmp/<slug>.md
```

`.tmp/` is gitignored. Never write scratch files to `/tmp` (invisible to the
user, lost on reboot, detached from the project that gives them meaning) and
never leave them loose in the repo root where they look like deliverables.

Implementation plans are the one exception — they keep their own home at
`.hermes/plans/YYYY-MM-DD_HHMMSS-<slug>.md` per the `plan` skill.


## Task tracking

Work is tracked on the `openlineage-spark` kanban board, bound to the
`openlineage-spark` Hermes project.

```bash
hermes kanban boards switch openlineage-spark
hermes kanban ls
hermes kanban show <task-id>
```
