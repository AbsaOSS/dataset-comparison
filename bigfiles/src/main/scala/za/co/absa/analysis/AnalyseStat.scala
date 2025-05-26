/**
 * Copyright 2020 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package za.co.absa.analysis

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
