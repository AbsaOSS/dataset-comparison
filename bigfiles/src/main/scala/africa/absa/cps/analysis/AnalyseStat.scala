package africa.absa.cps.analysis

import org.apache.spark.sql.Row

case class AnalyseStat(bestScore: Int, mask: Seq[Int], bestRowB: Row)
