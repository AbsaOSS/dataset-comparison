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

/** Result of row-by-row analysis with explicit outcome types.
  */
sealed trait AnalysisResult

object AnalysisResult {

  /** Analysis completed successfully with row-by-row differences.
    *
    * @param diffAToB
    *   Differences from dataset A to B
    * @param diffBToA
    *   Differences from dataset B to A
    */
  case class Success(diffAToB: Seq[RowsDiff], diffBToA: Seq[RowsDiff]) extends AnalysisResult

  /** Datasets are identical - no differences to analyze.
    */
  case object DatasetsIdentical extends AnalysisResult

  /** One dataset has differences but the other doesn't - cannot perform row-by-row matching.
    *
    * @param diffCountA
    *   Number of differences in dataset A
    * @param diffCountB
    *   Number of differences in dataset B
    */
  case class OneSidedDifference(diffCountA: Long, diffCountB: Long) extends AnalysisResult

  /** Number of differences exceeds the threshold.
    *
    * @param diffCountA
    *   Number of differences in dataset A
    * @param diffCountB
    *   Number of differences in dataset B
    * @param threshold
    *   The threshold that was exceeded
    */
  case class ThresholdExceeded(diffCountA: Long, diffCountB: Long, threshold: Int) extends AnalysisResult
}

