package africa.absa.cps.analysis

import org.apache.spark.sql.Row

/** Analyses information that are passed into functions and returns with best results.
  * @param bestScore
  *   represents score of difference between two rows. It is picked the best one, teh lowest score. For every difference
  *   in rows it is increased by 1, so best score is 1 for only one difference, the worst is the number of columns.
  * @param mask
  *   it is a sequence of 0 and 1, where 0 means no difference and 1 means difference. Then you can pick just different
  *   rows on behalf of this mask.
  * @param bestRowRight
  *   represents the best row from Right DataFrame that is the closest to the row from Left DataFrame.
  */
case class AnalyseStat(bestScore: Int, mask: Seq[Int], bestRowRight: Row)
