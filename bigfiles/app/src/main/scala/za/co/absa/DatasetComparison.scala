/** Copyright 2020 ABSA Group Limited
  *
  * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
  * the License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */

package za.co.absa

import za.co.absa.analysis.{AnalysisResult, ComparisonMetricsCalculator, RowByRowAnalysis}
import za.co.absa.parser.{ArgsParser, DiffComputeType}
import za.co.absa.io.IOHandler
import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.Paths

object DatasetComparison {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val conf      = ConfigFactory.load()
    val threshold = conf.getInt("dataset-comparison.analysis.diff-threshold")

    val arguments = ArgsParser.getArgs(args)

    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("DatasetComparator")
      .getOrCreate()

    // validate arguments
    ArgsParser.validate(arguments)

    // read data
    val rawDataA = IOHandler.sparkRead(arguments.inputA)
    val rawDataB = IOHandler.sparkRead(arguments.inputB)

    val (diffA, diffB) = Comparator.compare(rawDataA, rawDataB, arguments.exclude)

    // write diff files
    val out = arguments.out
    IOHandler.dfWrite(Paths.get(out, "inputA_differences").toString, diffA, arguments.outFormat)
    IOHandler.dfWrite(Paths.get(out, "inputB_differences").toString, diffB, arguments.outFormat)

    val metrics = ComparisonMetricsCalculator
      .calculate(rawDataA, rawDataB, diffA, diffB, arguments.exclude)
      .getOrElse(throw new RuntimeException("Failed to calculate metrics"))
    val metricsJson = MetricsSerializer.serialize(metrics)
    IOHandler.jsonWrite(Paths.get(out, "metrics.json").toString, metricsJson)

    arguments.diff match {
      case DiffComputeType.Row =>
        logger.info("Starting row-by-row analysis")
        RowByRowAnalysis.analyze(diffA, diffB, threshold) match {
          case AnalysisResult.Success(diffAToB, diffBToA) =>
            logger.info("Computing row-by-row differences")
            IOHandler.rowDiffWriteAsJson(Paths.get(out, "A_to_B_changes.json").toString, diffAToB)
            IOHandler.rowDiffWriteAsJson(Paths.get(out, "B_to_A_changes.json").toString, diffBToA)
            logger.info("Row-by-row analysis completed successfully")

          case AnalysisResult.DatasetsIdentical =>
            logger.info("Datasets are identical, no row-by-row analysis needed")

          case AnalysisResult.OneSidedDifference(countA, countB) =>
            logger.info(
              s"""Detailed analysis will not be computed - one-sided difference:
                 |A: ${if (countA == 0) "All rows matched" else s"$countA differences (see inputA_differences)"}
                 |B: ${if (countB == 0) "All rows matched" else s"$countB differences (see inputB_differences)"}
                 |Row-by-row matching requires differences in both datasets
                 |""".stripMargin
            )

          case AnalysisResult.ThresholdExceeded(countA, countB, thresh) =>
            logger.warn(
              s"""Row-by-row analysis skipped - threshold exceeded:
                 |A differences: $countA
                 |B differences: $countB
                 |Threshold: $thresh
                 |Details available in inputA_differences and inputB_differences files
                 |""".stripMargin
            )
        }
      case _ => logger.info("None DiffComputeType selected")
    }
  }
}
