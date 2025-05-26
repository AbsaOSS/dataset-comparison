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

import org.apache.spark.sql.{DataFrame, SparkSession}

import org.slf4j.{Logger, LoggerFactory}

object DatasetComparisonHelper {

  private var logger: Logger = LoggerFactory.getLogger(DatasetComparisonHelper.getClass)

  /** Exclude columns from a DataFrame
    *
    * @param df
    *   DataFrame to exclude columns from
    * @param toExclude
    *   columns to exclude
    * @param dfName
    *   name of the DataFrame
    * @param spark
    *   SparkSession
    * @return
    *   DataFrame with excluded columns
    */
  def exclude(df: DataFrame, toExclude: Seq[String], dfName: String)(implicit spark: SparkSession): DataFrame = {
    if (toExclude.isEmpty) df
    else {
      logger.info(s"Excluding columns from the $dfName DataFrame")
      val toExcludeColumns = df.columns.filter(toExclude.contains) // get columns to exclude that are in the DataFrame
      toExclude
        .filterNot(df.columns.contains)
        .foreach(col =>
          logger.warn(s"Column $col not found in the $dfName DataFrame")
        ) // log warning for columns that are not in the DataFrame
      df.drop(toExcludeColumns: _*)
    }
  }

}
