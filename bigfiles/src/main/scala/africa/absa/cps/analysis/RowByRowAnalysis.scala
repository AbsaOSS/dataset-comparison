package africa.absa.cps.analysis

import africa.absa.cps.hash.HashUtils.HASH_COLUMN_NAME
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.slf4j.{Logger, LoggerFactory}
import upickle.default._

import scala.annotation.tailrec

object RowByRowAnalysis {

  private val logger: Logger = LoggerFactory.getLogger(RowByRowAnalysis.getClass)


  /**
   * Recursively finds the best matching row in diffB
   * for the given rowA based on the minimum difference score.
   *
   * @param rowA The row from DataFrame A to compare.
   * @param diffB The DataFrame B to compare against.
   * @param stats The current best match statistics.
   * @return best match statistics. This contains the best score, the best row from DataFrame B
   *         and mask which has 0 and 1, 1 means the column is different.
   */
  @tailrec
  private def getBest(rowA: Row, diffB: DataFrame, stats: AnalyseStat): AnalyseStat = {
    logger.debug("Get the current row from DataFrame B")
    val rowB = diffB.head()
    val diffBTail = diffB.except(diffB.limit(1))

    logger.debug(s"Calculate the difference score between rowA ${rowA.toString()} and rowB ${rowB.toString()}")
    val diff = rowA.toSeq.zip(rowB.toSeq).map { case (a, b) => if (a == b) 0 else 1 }
    val score = diff.sum

    if (!diffBTail.isEmpty) {
      if (score < stats.bestScore) {
        logger.debug("Changing best score")
        getBest(rowA, diffBTail, AnalyseStat(bestScore = score, mask = diff, bestRowB = rowB) )
      }
      else {
        getBest(rowA, diffBTail, stats)
      }
    }
    else {
      logger.debug(s"Returning the best score stats")
      if (score < stats.bestScore) AnalyseStat(bestScore = score, mask = diff, bestRowB = rowB)  else stats
    }

  }

  /**
   * Recursively generates a JSON string representing the differences
   * between two DataFrames for a given row.
   *
   * @param maskedColumns The columns to compare.
   * @param maskedA masked row A to compare.
   * @param maskedB masked row B to compare against.
   * @param res The accumulated result string.
   * @return The JSON string representing the differences for the given row.
   */
  @tailrec
  private def getDiff(maskedColumns: Array[String], maskedA: Seq[Any], maskedB: Seq[Any], res: List[ColumnsDiff] = List()): List[ColumnsDiff] = {
    if (maskedColumns.isEmpty) {
      res
    }
    else {
      val columnName = maskedColumns.head
      // Skip the hash column
      if (columnName == HASH_COLUMN_NAME) getDiff(maskedColumns.tail, maskedA.tail, maskedB.tail, res)
      else {
        val a = maskedA.head
        val b = maskedB.head
        getDiff(maskedColumns.tail, maskedA.tail, maskedB.tail, res :+ ColumnsDiff(columnName = columnName, values = List(a.toString, b.toString)))
      }
    }
  }

  /**
   * Apply the mask  that was created by getBest to the columns of the DataFrame and rows A and B.
   * By applying the mask we will pick only the values that are different.
   * @param diffA The DataFrame A to compare.
   * @param rowA The current row in DataFrame A.
   * @param rowB The current row in DataFrame B.
   * @param mask The mask to apply.
   * @return masked columns, masked row A and masked row B
   */
  private def getMasked(diffA: DataFrame, rowA: Row, rowB: Row, mask: Seq[Int]):(Array[String], Seq[Any], Seq[Any] ) = {
    val maskedColumns = diffA.columns.zip(mask).collect {
      case (col, 1) => col
    }
    val maskedA = rowA.toSeq.zip(mask).collect {
      case (col, 1) => col
    }
    val maskedB = rowB.toSeq.zip(mask).collect {
      case (col, 1) => col
    }
    (maskedColumns, maskedA, maskedB)
  }

  /**
   * Recursively generates a JSON string representing the differences between two DataFrames.
   *
   * @param diffA The DataFrame A to compare.
   * @param indexA The current index in DataFrame A.
   * @param diffB The DataFrame B to compare against.
   * @param name The name identifier for the DataFrame.
   * @param res The accumulated result string.
   * @return The List[RowsDiff] representing the differences.
   */
  @tailrec
  private def generateDiffJson(diffA: DataFrame, indexA: Int, diffB: DataFrame, name: String, res: List[RowsDiff] = List()): List[RowsDiff] = {
    val rowA = diffA.head()
    val diffATail = diffA.filter(row => row != rowA)

    logger.info(s"Compute best match for row: ${rowA.toString()}")
    val best = getBest(rowA, diffB, AnalyseStat(bestScore = rowA.length + 1, mask = Seq[Int](), bestRowB = Row()))

    logger.debug(s"${best.bestScore} score for row ${rowA} in ${name}, row B ${best.bestRowB}, mask ${best.mask}\n")
    logger.info("Get the hash values for the rows")
    val hashA = rowA.getAs[Int](HASH_COLUMN_NAME)
    val hashB = best.bestRowB.getAs[Int](HASH_COLUMN_NAME)

    logger.info("Applying mask to columns")
    val (maskedColumns, maskedA, maskedB) = getMasked(diffA, rowA, best.bestRowB, best.mask)

    logger.info("Computing the difference")
    val diffForRow = RowsDiff(inputAHash = hashA.toString, inputBHash = hashB.toString, diffs = getDiff(maskedColumns, maskedA, maskedB))
    if (!diffATail.isEmpty) {
      generateDiffJson(diffATail, indexA + 1, diffB, name, res :+ diffForRow)
    } else {
      res :+ diffForRow
    }
  }


  /**
   * Analyse the differences between two DataFrames
   * @param diffA The DataFrame A to compare.
   * @param diffB  The DataFrame B to compare against.
   * @return List[RowsDiff] containing the differences between the two DataFrames A to B.
   *
   */
  def analyse(diffA: DataFrame, diffB: DataFrame, name: String): List[RowsDiff] = {
    logger.info(s"Row by row analysis for ${name}")
    generateDiffJson(diffA, 0, diffB, name)
  }
}
