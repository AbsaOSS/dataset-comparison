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

import za.co.absa.DatasetComparison.logger
import za.co.absa.analysis.RowByRowAnalysis
import za.co.absa.parser.{ArgsParser, DiffComputeType}
import za.co.absa.io.IOHandler
import org.apache.spark.sql.{DataFrame, SparkSession}
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
    val rawDataA         = IOHandler.sparkRead(arguments.inputA)
    val rawDataB         = IOHandler.sparkRead(arguments.inputB)
    val dataA: DataFrame = DatasetComparisonHelper.exclude(rawDataA, arguments.exclude, "A")
    val dataB: DataFrame = DatasetComparisonHelper.exclude(rawDataB, arguments.exclude, "B")

    val (uniqA, uniqB) = Comparator.compare(dataA, dataB)

    val metrics: String = Comparator.createMetrics(dataA, dataB, uniqA, uniqB, arguments.exclude)

    // write to files
    val out = arguments.out
    IOHandler.dfWrite(Paths.get(out, "inputA_differences").toString, uniqA, arguments.outFormat)
    IOHandler.dfWrite(Paths.get(out, "inputB_differences").toString, uniqB, arguments.outFormat)
    IOHandler.jsonWrite(Paths.get(out, "metrics.json").toString, metrics)

    val uniqAEmpty = uniqA.isEmpty
    val uniqBEmpty = uniqB.isEmpty
    arguments.diff match {
      case _ if uniqBEmpty || uniqAEmpty => logEitherUniqEmpty(uniqAEmpty, uniqBEmpty)
      case DiffComputeType.Row           => handleRowDiffType(uniqA, uniqB, out, threshold)
      case _                             => logger.info("None DiffComputeType selected")
    }
  }

  private def handleRowDiffType(uniqA: DataFrame, uniqB: DataFrame, out: String, threshold: Int)(implicit
      sparkSession: SparkSession
  ): Unit = {
    if (uniqA.count() <= threshold && uniqB.count() <= threshold) {
      val diffA = RowByRowAnalysis.generateDiffJson(uniqA, uniqB, "A")
      val diffB = RowByRowAnalysis.generateDiffJson(uniqB, uniqA, "B")

      // write diff
      IOHandler.rowDiffWriteAsJson(Paths.get(out, "A_to_B_changes.json").toString, diffA)
      IOHandler.rowDiffWriteAsJson(Paths.get(out, "B_to_A_changes.json").toString, diffB)
    } else {
      logger.warn("The number of differences is too large to compute row by row differences.")
    }
  }

  private def logEitherUniqEmpty(uniqAEmpty: Boolean, uniqBEmpty: Boolean): Unit = {
    logger.info(
      s"""Detailed analysis will not be computed:
         |A: ${if (uniqAEmpty) "All rows matched" else "There is no other row in B look to inputA_differences"}
         |B: ${if (uniqBEmpty) "All rows matched" else "There is no other row in A look to inputB_differences"}
         |""".stripMargin
    )
  }
}
