package africa.absa.cps.analysis

import africa.absa.cps.hash.HashUtils.HASH_COLUMN_NAME
import org.apache.spark.sql.{DataFrame, Row}
import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec

object RowByRowAnalysis {

  private val logger: Logger = LoggerFactory.getLogger(RowByRowAnalysis.getClass)

  /**
   * Recursively finds the best matching row in diffB
   * for the given rowA based on the minimum difference score.
   *
   * @param rowA The row from DataFrame A to compare.
   * @param diffB The DataFrame B to compare against.
   * @param indexB The current index in DataFrame B.
   * @param stats The current best match statistics.
   * @return best match statistics.
   */
  @tailrec
  private def getBest(rowA: Row, diffB: DataFrame, indexB: Int, stats: AnalyseStat): AnalyseStat = {
    logger.debug("Get the current row from DataFrame B")
    val rowB = diffB.collect()(indexB)

    logger.debug(s"Calculate the difference score between rowA ${rowA.toString()} and rowB ${rowB.toString()}")
    val diff = rowA.toSeq.zip(rowB.toSeq).map { case (a, b) => if (a == b) 0 else 1 }
    val score = diff.sum

    if (indexB < diffB.count() - 1) {
      if (score < stats.bestScore) {
        logger.debug("Changing best score")
        getBest(rowA, diffB, indexB + 1, AnalyseStat(bestScore = score, mask = diff, index = indexB) )
      }
      else {
        getBest(rowA, diffB, indexB + 1, stats)
      }
    }
    else {
      logger.debug(s"Returning the best score stats")
      if (score < stats.bestScore) AnalyseStat(bestScore = score, mask = diff, index = indexB)  else stats
    }

  }

  /**
   * Recursively generates a JSON string representing the differences
   * between two DataFrames for a given row.
   *
   * @param maskedColumns The columns to compare.
   * @param diffA The DataFrame A to compare.
   * @param indexA The current index in DataFrame A.
   * @param diffB The DataFrame B to compare against.
   * @param res The accumulated result string.
   * @return The JSON string representing the differences for the given row.
   */
  @tailrec
  private def getDiff(maskedColumns: Array[String], diffA: DataFrame, indexA: Int, diffB: DataFrame, res: String = ""): String = {
    if (maskedColumns.isEmpty) {
      res
    }
    else {
      val columnName = maskedColumns.head
      // Skip the hash column
      if (columnName == HASH_COLUMN_NAME) getDiff(maskedColumns.tail, diffA, indexA, diffB, res)
      else {
        val a = diffA.select(columnName).collect()(indexA).get(0)
        val b = diffB.select(columnName).collect()(indexA).get(0)
        getDiff(maskedColumns.tail, diffA, indexA, diffB, s"""$res\"$columnName\": ["$a", "$b"], """)
      }
    }
  }

  /**
   * Recursively generates a JSON string representing the differences between two DataFrames.
   *
   * @param diffA The DataFrame A to compare.
   * @param indexA The current index in DataFrame A.
   * @param diffB The DataFrame B to compare against.
   * @param name The name identifier for the DataFrame.
   * @param res The accumulated result string.
   * @return The JSON string representing the differences.
   */
  @tailrec
  private def generateDiffJson(diffA: DataFrame, indexA: Int, diffB: DataFrame, name: String, res: String = "{"): String = {
    val rowA = diffA.collect()(indexA)
    logger.info(s"Compute best match for row: ${rowA.toString()}")
    val best = getBest(rowA, diffB, 0, AnalyseStat(bestScore = rowA.length + 1, mask = Seq[Int](), index = -1))

    logger.debug(s"${best.bestScore} score for row ${indexA} in ${name}, row index ${best.index}, mask ${best.mask}\n")
    logger.info("Get the hash values for the rows")
    val hashA = diffA.select(HASH_COLUMN_NAME).collect()(indexA).get(0)
    val hashB = diffB.select(HASH_COLUMN_NAME).collect()(best.index).get(0)
    logger.info("Applying mask to columns")
    val maskedColumns = diffA.columns.zip(best.mask).collect {
      case (col, 1) => col
    }
    logger.info("Computing the difference")
    val diffForRow = s""""${hashA} ${hashB}":{ ${getDiff(maskedColumns, diffA, indexA, diffB)}}, \n"""
    if (indexA < diffA.count() - 1) generateDiffJson(diffA, indexA + 1, diffB, name, res + diffForRow)
    else res + diffForRow + "}"
  }

  /**
   * Analyse the differences between two DataFrames
   * @param diffA The DataFrame A to compare.
   * @param diffB  The DataFrame B to compare against.
   * @return A tuple containing the differences between the two DataFrames,
   *         one for difference A to B, second for B to A.
   */
  def analyse(diffA: DataFrame, diffB: DataFrame): (String, String) = {
    logger.info("Row by row analysis")
    (generateDiffJson(diffA, 0, diffB, "A") , generateDiffJson(diffB, 0, diffA, "B"))
  }
}
