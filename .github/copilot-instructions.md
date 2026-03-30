Tool for exact comparison of two Parquet/CSV datasets, detecting row and column-level differences.
Monorepo: `bigfiles/` (Scala+Spark, files not fitting RAM) and `smallfiles/` (Python, files fitting RAM).

## Scala (bigfiles/)
- Scala 2.12.20 default, 2.11.12 cross-compiled via `sbt +`. Spark 3.5.3/2.4.7, Hadoop 3.3.5/2.6.5, Java 8.
- SBT 1.10.2. All sbt commands run from `bigfiles/`. Entry point: `za.co.absa.DatasetComparison`.
- `sbt test` — unit + integration (local Spark `local[*]`). `sbt jacoco` — coverage. `sbt assembly` — fat JAR.
- JaCoCo online mode: `sbt-jacoco` + `jacoco-method-filter-sbt`, rules in `bigfiles/jmf-rules.txt`, aliases in `bigfiles/.sbtrc`.
- scalafmt: dialect scala211 (cross-compat), maxColumn 120. `assemblyMergeStrategy` discards META-INF.
- No runtime services — pure Spark batch job.

## Python (smallfiles/)
- Python 3.13. Entry point: `smallfiles/main.py`. Deps pinned in `smallfiles/requirements.txt`.

## Quality gates
- Scala: JaCoCo overall >= 67% ( >= 80% is goal), changed files >= 80%. PR comments via `MoranaApps/jacoco-report`.
- Python: pytest >= 80%, pylint >= 9.5, black formatting, mypy type checking.

## Conventions
- Apache 2.0 license headers on all source files.
- Organization: `za.co.absa`. Git versioning via `sbt-git`.
- GH Actions: pinned commit SHAs for all third-party actions.
- `bigfiles/project/` — sbt build definitions only, excluded from CI change detection.
