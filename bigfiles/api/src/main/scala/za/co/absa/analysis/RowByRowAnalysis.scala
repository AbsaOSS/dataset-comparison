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

import za.co.absa.hash.HashUtils.HASH_COLUMN_NAME
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}
import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec

object RowByRowAnalysis {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /** Analyze comparison result and generate row-by-row differences if within threshold.
    *
    * @param diffA
    *   DataFrame containing rows unique to dataset A
    * @param diffB
    *   DataFrame containing rows unique to dataset B
    * @param threshold
    *   Maximum number of differences to analyze per dataset
    * @return
    *   AnalysisResult indicating the outcome: Success with diffs, DatasetsIdentical, OneSidedDifference, or ThresholdExceeded
    */
  def analyze(diffA: DataFrame, diffB: DataFrame, threshold: Int): AnalysisResult = {
    val diffCountA = diffA.count()
    val diffCountB = diffB.count()

    if (diffCountA == 0 && diffCountB == 0) {
      AnalysisResult.DatasetsIdentical
    } else if (diffCountA == 0 || diffCountB == 0) {
      AnalysisResult.OneSidedDifference(diffCountA, diffCountB)
    } else if (diffCountA > threshold || diffCountB > threshold) {
      AnalysisResult.ThresholdExceeded(diffCountA, diffCountB, threshold)
    } else {
      val diffAResult = generateDiffJson(diffA, diffB, "A")
      val diffBResult = generateDiffJson(diffB, diffA, "B")
      AnalysisResult.Success(diffAResult, diffBResult)
    }
  }

  /** Recursively finds the best matching row in diffRight for the given rowLeft based on the minimum difference score.
    *
    * @param rowLeft
    *   The row from the left DataFrame to compare
    * @param diffRight
    *   The right DataFrame to compare against
    * @param bestScore
    *   The best score found so far (lower is better)
    * @param mask
    *   The mask indicating which columns differ in the best match
    * @param bestRowRight
    *   The best matching row found so far
    * @return
    *   best match statistics. This contains the best score, the best row from DataFrame Right and mask which has 0 and
    *   1, 1 means the column is different. Mask represents the comparison between two rows where 0 means they are the
    *   same, while 1 means they differ. The sequence then gives the representation of the whole row of differences.
    */
  @tailrec
  private def getBest(
      rowLeft: Row,
      diffRight: DataFrame,
      bestScore: Int,
      mask: Seq[Int] = Seq.empty,
      bestRowRight: Row = Row.empty
  ): AnalyseStat = {
    logger.debug("Get the current row from DataFrame Right")
    val rowRight      = diffRight.head()
    val hashRight     = rowRight.getAs[Int](HASH_COLUMN_NAME)
    val diffRightTail = diffRight.filter(col(HASH_COLUMN_NAME).notEqual(hashRight))

    logger.debug(
      s"Calculate the difference score between rowLeft ${rowLeft.toString()} and rowRight ${rowRight.toString()}"
    )
    val diff  = rowLeft.toSeq.zip(rowRight.toSeq).map { case (a, b) => if (a == b) 0 else 1 }
    val score = diff.sum

    val (newBestScore: Int, newMask: Seq[Int], newBestRow: Row) = if (score < bestScore) {
      logger.debug("Changing best score")
      (score, diff, rowRight)
    } else {
      (bestScore, mask, bestRowRight)
    }

    if (!diffRightTail.isEmpty) {
      getBest(rowLeft, diffRightTail, newBestScore, newMask, newBestRow)
    } else {
      logger.debug(s"Returning the best score stats")
      AnalyseStat(newBestScore, newMask, newBestRow)
    }

  }

  /** Apply the mask that was created by getBest to provided sequence. By applying the mask we will pick only the values
    * that are different.
    * @param data
    *   sequence on which mask should be apply
    * @param mask
    *   The mask to apply.
    * @return
    *   masked columns, masked row Left and masked row Right
    */
  private def getDifferencesByMask[T](data: Seq[T], mask: Seq[Int]): Seq[T] = {
    data.zip(mask).collect { case (col, 1) => col }
  }

  /** Recursively generates a JSON string representing the differences between two DataFrames.
    *
    * @param diffLeft
    *   The DataFrame Left to compare.
    * @param indexLeft
    *   The current index in DataFrame Left.
    * @param diffRight
    *   The DataFrame Right to compare against.
    * @param name
    *   The name identifier for the DataFrame.
    * @param res
    *   The accumulated result string.
    * @return
    *   Seq[RowsDiff] containing the differences between the two DataFrames Left to Right.
    */
  @tailrec
  def generateDiffJson(
      diffLeft: DataFrame,
      diffRight: DataFrame,
      name: String,
      indexLeft: Int = 0,
      res: Seq[RowsDiff] = Seq.empty
  ): Seq[RowsDiff] = {
    val rowLeft      = diffLeft.head()
    val hashLeft     = rowLeft.getAs[Int](HASH_COLUMN_NAME)
    val diffLeftTail = diffLeft.filter(col(HASH_COLUMN_NAME).notEqual(hashLeft))

    logger.info(s"Compute best match for row: ${rowLeft.toString()}")
    val best: AnalyseStat = getBest(rowLeft, diffRight, rowLeft.length + 1)

    logger.debug(
      s"${best.bestScore} score for row $rowLeft in $name, row Right ${best.bestRowRight}, mask ${best.mask}\n"
    )
    logger.info("Get the hash values for the rows")
    val hashRight = best.bestRowRight.getAs[Int](HASH_COLUMN_NAME)

    logger.info("Applying mask to columns")
    val diffsColumns = getDifferencesByMask(diffLeft.columns, best.mask)
    val diffsLeft    = getDifferencesByMask(rowLeft.toSeq, best.mask)
    val diffsRight   = getDifferencesByMask(best.bestRowRight.toSeq, best.mask)

    logger.info("Computing the difference")
    val diffs = diffsColumns.zip(diffsLeft.zip(diffsRight)).collect {
      case (columnName, (valLeft, valRight)) if columnName != HASH_COLUMN_NAME =>
        ColumnsDiff(columnName, Seq(valLeft.toString, valRight.toString))
    }
    val diffForRow = RowsDiff(hashLeft.toString, hashRight.toString, diffs)

    if (!diffLeftTail.isEmpty) {
      generateDiffJson(diffLeftTail, diffRight, name, indexLeft + 1, res :+ diffForRow)
    } else {
      res :+ diffForRow
    }
  }

}
