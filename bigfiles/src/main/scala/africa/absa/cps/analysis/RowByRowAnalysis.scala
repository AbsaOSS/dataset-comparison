package africa.absa.cps.analysis

import africa.absa.cps.hash.HashUtils.HASH_COLUMN_NAME
import org.apache.spark.sql.{DataFrame, Row}
import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec

object RowByRowAnalysis {

  private val logger: Logger = LoggerFactory.getLogger(RowByRowAnalysis.getClass)
  private val NULL: Int = -1

  @tailrec
  private def getBest(rowA: Row, diffB: DataFrame, indexB: Int, stats: AnalyseStat): AnalyseStat= {
    val rowB = diffB.collect()(indexB)
    val diff = rowA.toSeq.zip(rowB.toSeq).map { case (a, b) => if (a == b) 0 else 1 }
    val score = diff.sum
    if (indexB < diffB.count() - 1) {
      if (score < stats.bestScore) {
        getBest(rowA, diffB, indexB + 1, AnalyseStat(bestScore = score, mask = diff, index = indexB) )
      }
      else {
        getBest(rowA, diffB, indexB + 1, stats)
      }
    }
    else {
      if (score < stats.bestScore) AnalyseStat(bestScore = score, mask = diff, index = indexB)  else stats
    }

  }
  @tailrec
  private def tmp(diffA: DataFrame, indexA: Int, diffB: DataFrame, name: String): Unit = {
    val rowA = diffA.collect()(indexA)

    val best = getBest(rowA, diffB, 0, AnalyseStat(bestScore = rowA.length + 1, mask = Seq[Int](), index = -1))

    print(s"${best.bestScore} score for row ${indexA} in ${name}, row index ${best.index}, mask ${best.mask}\n")
    val hashA = diffA.select(HASH_COLUMN_NAME).collect()(indexA).get(0)
    val hashB = diffB.select(HASH_COLUMN_NAME).collect()(best.index).get(0)
    val maskedColumns = diffA.columns.zip(best.mask).collect {
      case (col, 1) => col
    }
//    print(s"${hashA} ${hashB}: \n")
    // hash1a hash1b: column3 (value1 != value2),  column203 (value1 != value2)
    if (indexA < diffA.count() - 1) {
      tmp(diffA, indexA + 1, diffB, name)
    }
  }

  def analyse(diffA: DataFrame, diffB: DataFrame): Unit = {
    logger.info("Row by row analysis")
    tmp(diffA, 0, diffB, "A")
    tmp(diffB, 0, diffA, "B")
  }
}
