/**
 * Copyright 2020 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package africa.absa.cps.hash

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, struct, udf}
import org.slf4j.{Logger, LoggerFactory}

object HashUtils {

  val HASH_COLUMN_NAME = "cps_comparison_hash"

  private val logger: Logger = LoggerFactory.getLogger(HashUtils.getClass)
  private val hashUDF        = udf((row: Row) => hashRow(row))

  /** Hash a row
    *
    * @param row
    *   Row to hash
    * @return
    *   Hash of the row
    */
  private def hashRow(row: Row): Int = row.mkString.hashCode

  /** Hash the dataframe
    *
    * @param df
    *   DataFrame to hash
    * @return
    *   DataFrame with hash column
    */
  def createHashColumn(df: DataFrame): DataFrame = {
    logger.info("Hashing the dataframe")
    df.withColumn(HASH_COLUMN_NAME, hashUDF(struct(df.columns.map(col): _*)))
  }

}
