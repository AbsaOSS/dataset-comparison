package africa.absa.cps

import hash.HashDataFrame
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.native.JsonMethods.{compact, render}

import org.json4s.JsonDSL._

/**
 * Comparator object that compares two parquet files and creates metrics
 */
object Comparator {

  /**
   * Create metrics
   *
   * @param dataA A DataFrame whole data
   * @param dataB B DataFrame whole data
   * @param uniqA only unique rows in A DataFrame
   * @param uniqB only unique rows in B DataFrame
   * @return JSON string with metrics
   */
  def createMetrics(dataA: DataFrame, dataB: DataFrame, uniqA: DataFrame, uniqB: DataFrame): String = {

    // compute metrics
    val rowCountA = dataA.count()
    val rowCountB = dataB.count()
    val uniqCountA = uniqA.count()
    val uniqCountB = uniqB.count()
    val sameRecords = if (rowCountA - uniqCountA == rowCountB - uniqCountB) rowCountA - uniqCountA else -1

    val metricsJson =
      ("A" ->
        ("row count" -> rowCountA) ~
        ("column count" -> dataA.columns.length) ~
        ("diff column count" -> uniqCountA)) ~
      ("B" ->
          ("row count" -> rowCountB) ~
          ("column count" -> dataB.columns.length) ~
          ("diff column count" -> uniqCountB)) ~
      ("general" ->
          ("same records count" -> sameRecords) ~
          ("same records percent" -> (math floor (sameRecords.toFloat/rowCountA)*10000)/100))

    compact(render(metricsJson))
  }

  /**
   * Compare two parquet files and return unique rows
   *
   * @param dataA dataframe from input A
   * @param dataB dataframe from input B
   * @param outputPath to output folder
   * @param spark SparkSession
   * @return two DataFrames with unique rows
   */
  def compare(dataA: DataFrame, dataB: DataFrame, outputPath: String)(implicit spark: SparkSession): (DataFrame, DataFrame) = {

    // preprocess data todo will be solved by issue #5

    // compute hash rows
    val dfWithHashA: DataFrame = HashDataFrame.hash(dataA)
    val dfWithHashB: DataFrame = HashDataFrame.hash(dataB)

    // select non matching hashs
    val uniqHashA: DataFrame = dfWithHashA.select(Main.HashName).exceptAll(dfWithHashB.select(Main.HashName))
    val uniqHashB: DataFrame = dfWithHashB.select(Main.HashName).exceptAll(dfWithHashA.select(Main.HashName))

    // join on hash column (get back whole rows)
    val uniqA: DataFrame = dfWithHashA.join(uniqHashA, Seq(Main.HashName))
    val uniqB: DataFrame = dfWithHashB.join(uniqHashB, Seq(Main.HashName))

    (uniqA, uniqB)
  }

}
