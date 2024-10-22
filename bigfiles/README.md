# Scala CPS-Dataset-Comparison 

This is scala implementation of the project. It is used for comparing big files.

- [How to run](#how-to-run)
  - [Requirements](#requirements)
- [How to run tests](#how-to-run-tests)

## How to run

First run assembly: `sbt assembly`

Then run:

```bash
spark-submit target/scala-2.12/dataset-comparison-assembly-1.0.jar -o <output-path> --inputA <A-file-path> --inputB <B-file-path> 
```

### Requirements

- scala 2.12
- spark 3.5.3
- java 17
- 
more requirements are in [Dependency](project/Dependencies.scala) file
## How to run tests


| sbt command | test type | info                                   |
| ----------- |-----------|----------------------------------------|
| `sbt test`  | ...       | It will run tests in test/scala folder |
