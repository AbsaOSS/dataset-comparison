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

import org.json4s.JsonAST
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods.{compact, render}
import za.co.absa.analysis.ComparisonMetrics

/** Serializes ComparisonMetrics to JSON format.
  */
object MetricsSerializer {

  /** Converts ComparisonMetrics to a JSON object.
    *
    * @param metrics
    *   The comparison metrics to serialize
    * @return
    *   JSON object representation of the metrics
    */
  def toJson(metrics: ComparisonMetrics): JsonAST.JObject = {
    ("A" ->
      ("row count"             -> metrics.rowCountA) ~
      ("column count"          -> metrics.columnCountA) ~
      ("rows not present in B" -> metrics.diffCountA) ~
      ("unique rows count"     -> metrics.uniqueRowCountA)) ~
    ("B" ->
      ("row count"             -> metrics.rowCountB) ~
      ("column count"          -> metrics.columnCountB) ~
      ("rows not present in A" -> metrics.diffCountB) ~
      ("unique rows count"     -> metrics.uniqueRowCountB)) ~
    ("general" ->
      ("same records count"        -> metrics.sameRecordsCount) ~
      ("same records percent to A" -> metrics.sameRecordsPercentToA) ~
      ("excluded columns"          -> metrics.excludedColumns.mkString(", ")))
  }

  /** Serializes ComparisonMetrics to a compact JSON string.
    *
    * @param metrics
    *   The comparison metrics to serialize
    * @return
    *   Compact JSON string representation of the metrics
    */
  def serialize(metrics: ComparisonMetrics): String = {
    compact(render(toJson(metrics)))
  }
}
