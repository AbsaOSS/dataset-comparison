package africa.absa.cps

import hash.HashUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.native.JsonMethods.{compact, render}
import org.json4s.JsonDSL._
import org.slf4j.{Logger, LoggerFactory}

/**
 * Comparator object that compares two parquet files and creates metrics
 */
object Comparator {

  val HashName = "cps_comparison_hash"
  private val logger: Logger = LoggerFactory.getLogger(Comparator.getClass)

  /**
   * Create metrics
   *
   * @param dataA A DataFrame whole data
   * @param dataB B DataFrame whole data
   * @param diffA only diff rows in A DataFrame
   * @param diffB only diff rows in B DataFrame
   * @return JSON string with metrics
   */
  def createMetrics(dataA: DataFrame, dataB: DataFrame, diffA: DataFrame, diffB: DataFrame): String = {

    logger.info("Computing metrics")

    // compute metrics
    val rowCountA = dataA.count()
    val rowCountB = dataB.count()
    val uniqRowCountA = dataA.distinct().count()
    val uniqRowCountB = dataB.distinct().count()
    val diffCountA = diffA.count()
    val diffCountB = diffB.count()
    val sameRecords = if (rowCountA - diffCountA == rowCountB - diffCountB) rowCountA - diffCountA else -1

    val metricsJson =
      ("A" ->
        ("row count" -> rowCountA) ~
        ("column count" -> dataA.columns.length) ~
        ("rows not present in B" -> diffCountA)) ~
        ("unique rows count" -> uniqRowCountA) ~
      ("B" ->
          ("row count" -> rowCountB) ~
          ("column count" -> dataB.columns.length) ~
          ("rows not present in A" -> diffCountB)) ~
          ("unique rows count" -> uniqRowCountB) ~
      ("general" ->
          ("same records count" -> sameRecords) ~
          ("same records percent" -> (math floor (sameRecords.toFloat/rowCountA)*10000)/100))

    compact(render(metricsJson))
  }

  /**
   * Compare two parquet files and return diff rows
   *
   * @param dataA dataframe from input A
   * @param dataB dataframe from input B
   * @param spark SparkSession
   * @return two DataFrames with diff rows
   */
  def compare(dataA: DataFrame, dataB: DataFrame)(implicit spark: SparkSession): (DataFrame, DataFrame) = {

    // preprocess data todo will be solved by issue #5

    // compute hash rows
    val dfWithHashA: DataFrame = HashUtils.createHashColumn(dataA)
    logger.info("Hash for A created")
    val dfWithHashB: DataFrame = HashUtils.createHashColumn(dataB)
    logger.info("Hash for B created")

    // select non matching hashs
    logger.info("Getting diff hashes, A except B")
    val diffHashA: DataFrame = dfWithHashA.select(HashName).exceptAll(dfWithHashB.select(HashName))
    logger.info("Getting diff hashes, B except A")
    val diffHashB: DataFrame = dfWithHashB.select(HashName).exceptAll(dfWithHashA.select(HashName))

    // join on hash column (get back whole rows)
    logger.info("Getting diff rows for A")
    val diffA: DataFrame = dfWithHashA.join(diffHashA, Seq(HashName))
    logger.info("Getting diff rows for B")
    val diffB: DataFrame = dfWithHashB.join(diffHashB, Seq(HashName))

    (diffA, diffB)
  }

}
