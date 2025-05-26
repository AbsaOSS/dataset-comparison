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

import za.co.absa.hash.HashUtils.HASH_COLUMN_NAME
import hash.HashUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.JsonAST
import org.json4s.native.JsonMethods.{compact, render}
import org.json4s.JsonDSL._
import org.slf4j.{Logger, LoggerFactory}

/** Comparator object that compares two parquet files and creates metrics
  */
object Comparator {

  private val logger: Logger = LoggerFactory.getLogger(Comparator.getClass)

  /** Create metrics
    *
    * @param dataA
    *   A DataFrame whole data
    * @param dataB
    *   B DataFrame whole data
    * @param diffA
    *   only diff rows in A DataFrame
    * @param diffB
    *   only diff rows in B DataFrame
    * @param excludedColumns
    *   columns that were excluded from comparison
    * @return
    *   JSON string with metrics
    */
  def createMetrics(
      dataA: DataFrame,
      dataB: DataFrame,
      diffA: DataFrame,
      diffB: DataFrame,
      excludedColumns: Seq[String]
  ): String = {

    logger.info("Computing metrics")

    // compute metrics
    val rowCountA     = dataA.count()
    val rowCountB     = dataB.count()
    val uniqRowCountA = dataA.distinct().count()
    val uniqRowCountB = dataB.distinct().count()
    val diffCountA    = diffA.count()
    val diffCountB    = diffB.count()
    val sameRecords   = if (rowCountA - diffCountA == rowCountB - diffCountB) rowCountA - diffCountA else -1

    val metricsJson: JsonAST.JObject =
      ("A" ->
        ("row count"             -> rowCountA) ~
        ("column count"          -> dataA.columns.length) ~
        ("rows not present in B" -> diffCountA) ~
        ("unique rows count"     -> uniqRowCountA)) ~
        ("B" ->
          ("row count"             -> rowCountB) ~
          ("column count"          -> dataB.columns.length) ~
          ("rows not present in A" -> diffCountB) ~
          ("unique rows count"     -> uniqRowCountB)) ~
        ("general" ->
          ("same records count"        -> sameRecords) ~
          ("same records percent to A" -> (math floor (sameRecords.toFloat / rowCountA) * 10000) / 100) ~
          ("excluded columns"          -> excludedColumns.mkString(", ")))

    compact(render(metricsJson))
  }

  /** Compare two parquet files and return diff rows
    *
    * @param dataA
    *   dataframe from input A
    * @param dataB
    *   dataframe from input B
    * @param spark
    *   SparkSession
    * @return
    *   two DataFrames with diff rows
    */
  def compare(dataA: DataFrame, dataB: DataFrame)(implicit spark: SparkSession): (DataFrame, DataFrame) = {

    // preprocess data todo will be solved by issue #5

    // compute hash rows
    val dfWithHashA: DataFrame = HashUtils.createHashColumn(dataA)
    logger.info("Hash for A created")
    val dfWithHashB: DataFrame = HashUtils.createHashColumn(dataB)
    logger.info("Hash for B created")

    // select non matching hashs
    logger.info("Getting diff hashes, A except B")
    val diffHashA: DataFrame = dfWithHashA.select(HASH_COLUMN_NAME).exceptAll(dfWithHashB.select(HASH_COLUMN_NAME))
    logger.info("Getting diff hashes, B except A")
    val diffHashB: DataFrame = dfWithHashB.select(HASH_COLUMN_NAME).exceptAll(dfWithHashA.select(HASH_COLUMN_NAME))

    // join on hash column (get back whole rows)
    logger.info("Getting diff rows for A")
    val distinctDiffA: DataFrame = diffHashA.join(dfWithHashA, Seq(HASH_COLUMN_NAME)).distinct()
    val diffA: DataFrame         = diffHashA.join(distinctDiffA, Seq(HASH_COLUMN_NAME))

    logger.info("Getting diff rows for B")
    val distinctDiffB: DataFrame = diffHashB.join(dfWithHashB, Seq(HASH_COLUMN_NAME)).distinct()
    val diffB: DataFrame         = diffHashB.join(distinctDiffB, Seq(HASH_COLUMN_NAME))

    (diffA, diffB)
  }

}
