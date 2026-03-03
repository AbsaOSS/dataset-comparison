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
import org.slf4j.{Logger, LoggerFactory}

/** Comparator object that compares two parquet files and creates metrics
  */
object Comparator {
  private val logger: Logger = LoggerFactory.getLogger(Comparator.getClass)

  /** Compare two DataFrames and return diff rows
    *
    * @param dataA
    *   A DataFrame whole data
    * @param dataB
    *   B DataFrame whole data
    * @param excludedColumns
    *   columns to exclude from comparison
    * @param spark
    *   SparkSession
    * @return
    *   Tuple of (diffA, diffB) DataFrames containing rows unique to each dataset
    */
  def compare(
      dataA: DataFrame,
      dataB: DataFrame,
      excludedColumns: Seq[String] = Seq.empty
  )(implicit spark: SparkSession): (DataFrame, DataFrame) = {
    // Apply column exclusion
    val filteredDataA = DatasetComparisonHelper.exclude(dataA, excludedColumns, "A")
    val filteredDataB = DatasetComparisonHelper.exclude(dataB, excludedColumns, "B")

    val dfWithHashA: DataFrame = HashUtils.createHashColumn(filteredDataA)
    logger.info("Hash for A created")

    val dfWithHashB: DataFrame = HashUtils.createHashColumn(filteredDataB)
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
