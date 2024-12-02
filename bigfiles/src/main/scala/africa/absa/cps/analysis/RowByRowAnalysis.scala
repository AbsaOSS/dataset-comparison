package africa.absa.cps.analysis

import africa.absa.cps.hash.HashUtils.HASH_COLUMN_NAME
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}
import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec

object RowByRowAnalysis {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /** Recursively finds the best matching row in diffB for the given rowA based on the minimum difference score.
    *
    * @param rowA
    *   The row from DataFrame A to compare.
    * @param diffB
    *   The DataFrame B to compare against.
    * @param stats
    *   The current best match statistics.
    * @return
    *   best match statistics. This contains the best score, the best row from DataFrame B and mask which has 0 and 1, 1
    *   means the column is different. Mask represents the comparison between two rows where 0 means they are the same,
    *    while 1 means they differ. The sequence then gives the representation of the whole row of differences.
    */
  @tailrec
  private def getBest(rowA: Row,
                      diffB: DataFrame,
                      bestScore: Int,
                      mask: Seq[Int] = Seq.empty,
                      bestRowB: Row = Row.empty): AnalyseStat = {
    logger.debug("Get the current row from DataFrame B")
    val rowB = diffB.head()
    val hashB = rowB.getAs[Int](HASH_COLUMN_NAME)
    val diffBTail = diffB.filter(col(HASH_COLUMN_NAME).notEqual(hashB))

    logger.debug(s"Calculate the difference score between rowA ${rowA.toString()} and rowB ${rowB.toString()}")
    val diff  = rowA.toSeq.zip(rowB.toSeq).map { case (a, b) => if (a == b) 0 else 1 }
    val score = diff.sum

    val (newBestScore: Int, newMask: Seq[Int], newBestRow: Row) = if (score < bestScore) {
      logger.debug("Changing best score")
      (score, diff, rowB)
    } else {
      (bestScore, mask, bestRowB)
    }

    if (!diffBTail.isEmpty) {
      getBest(rowA, diffBTail, newBestScore, newMask, newBestRow)
    } else {
      logger.debug(s"Returning the best score stats")
      AnalyseStat(newBestScore, newMask, newBestRow)
    }

  }

  /** Apply the mask that was created by getBest to provided sequence. By applying the mask
    * we will pick only the values that are different.
    * @param data sequence on which mask should be apply
    * @param mask
    *   The mask to apply.
    * @return
    *   masked columns, masked row A and masked row B
    */
  private def getDifferencesByMask[T](data: Seq[T], mask: Seq[Int]): Seq[T] = {
    data.zip(mask).collect { case (col, 1) => col }
  }

  /** Recursively generates a JSON string representing the differences between two DataFrames.
    *
    * @param diffA
    *   The DataFrame A to compare.
    * @param indexA
    *   The current index in DataFrame A.
    * @param diffB
    *   The DataFrame B to compare against.
    * @param name
    *   The name identifier for the DataFrame.
    * @param res
    *   The accumulated result string.
    * @return
    *   The Seq[RowsDiff] representing the differences.
    */
  @tailrec
  private def generateDiffJson(
      diffA: DataFrame,
      indexA: Int,
      diffB: DataFrame,
      name: String,
      res: Seq[RowsDiff] = Seq.empty
  ): Seq[RowsDiff] = {
    val rowA = diffA.head()
    val hashA = rowA.getAs[Int](HASH_COLUMN_NAME)
    val diffATail = diffA.filter(col(HASH_COLUMN_NAME).notEqual(hashA))

    logger.info(s"Compute best match for row: ${rowA.toString()}")
    val best: AnalyseStat = getBest(rowA, diffB, rowA.length + 1)

    logger.debug(s"${best.bestScore} score for row ${rowA} in ${name}, row B ${best.bestRowB}, mask ${best.mask}\n")
    logger.info("Get the hash values for the rows")
    val hashB = best.bestRowB.getAs[Int](HASH_COLUMN_NAME)

    logger.info("Applying mask to columns")
    val diffsColumns = getDifferencesByMask(diffA.columns, best.mask)
    val diffsA = getDifferencesByMask(rowA.toSeq,best.mask)
    val diffsB = getDifferencesByMask(best.bestRowB.toSeq, best.mask)

    logger.info("Computing the difference")
    val diffs = diffsColumns.zip(diffsA.zip(diffsB)).collect {
      case (columnName, (valA, valB)) if columnName != HASH_COLUMN_NAME => ColumnsDiff(columnName, Seq(valA.toString, valB.toString))
    }
    val diffForRow = RowsDiff(hashA.toString, hashB.toString, diffs)

    if (!diffATail.isEmpty) {
      generateDiffJson(diffATail, indexA + 1, diffB, name, res :+ diffForRow)
    } else {
      res :+ diffForRow
    }
  }

  /** Analyse the differences between two DataFrames, runs generateDiffJson with index 0
    * @param diffA
    *   The DataFrame A to compare.
    * @param diffB
    *   The DataFrame B to compare against.
    * @return
    *   Seq[RowsDiff] containing the differences between the two DataFrames A to B.
    */
  def analyse(diffA: DataFrame, diffB: DataFrame, name: String): Seq[RowsDiff] = {
    logger.info(s"Row by row analysis for ${name}")
    generateDiffJson(diffA, 0, diffB, name)
  }
}
