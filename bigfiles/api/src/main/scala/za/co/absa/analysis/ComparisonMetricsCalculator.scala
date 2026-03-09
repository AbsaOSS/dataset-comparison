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

package za.co.absa.analysis

import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.DatasetComparisonHelper

/** Default implementation of ComparisonMetricsCalculator.
  *
  * Always returns metrics, including for identical datasets.
  */
object ComparisonMetricsCalculator {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def calculate(
      originalA: DataFrame,
      originalB: DataFrame,
      diffA: DataFrame,
      diffB: DataFrame,
      excludedColumns: Seq[String]
  ): Option[ComparisonMetrics] = {

    logger.info("Computing comparison metrics")

    val dataA = DatasetComparisonHelper.exclude(originalA, excludedColumns, "A")
    val dataB = DatasetComparisonHelper.exclude(originalB, excludedColumns, "B")

    val diffCountA = diffA.count()
    val diffCountB = diffB.count()

    logger.info(s"Diff counts - A: $diffCountA, B: $diffCountB")

    val rowCountA     = dataA.count()
    val rowCountB     = dataB.count()
    val uniqRowCountA = dataA.distinct().count()
    val uniqRowCountB = dataB.distinct().count()

    val sameRecords = if (rowCountA - diffCountA == rowCountB - diffCountB) {
      rowCountA - diffCountA
    } else {
      -1
    }

    val sameRecordsPercent = if (rowCountA > 0) {
      (math.floor(sameRecords.toFloat / rowCountA * 10000)) / 100
    } else {
      0.0
    }

    Some(
      ComparisonMetrics(
        rowCountA = rowCountA,
        rowCountB = rowCountB,
        columnCountA = dataA.columns.length,
        columnCountB = dataB.columns.length,
        diffCountA = diffCountA,
        diffCountB = diffCountB,
        uniqueRowCountA = uniqRowCountA,
        uniqueRowCountB = uniqRowCountB,
        sameRecordsCount = sameRecords,
        sameRecordsPercentToA = sameRecordsPercent,
        excludedColumns = excludedColumns
      )
    )
  }
}
