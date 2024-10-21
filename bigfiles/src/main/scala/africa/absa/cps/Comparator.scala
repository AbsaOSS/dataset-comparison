package africa.absa.cps

import hash.HashTable
import readers.SparkReader
import writers.SparkWriter
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.native.JsonMethods.{compact, render}

import java.nio.file.{Files, Paths}
import scala.util.matching.Regex

import org.json4s.JsonDSL._

/**
 * Comparator object that compares two parquet files and writes the differences to a folder
 */
object Comparator {
  /**
   * Extract the name of the file from the path
   *
   * @param valueString path to file
   * @return name of the file
   */
  private def extractName(valueString: String): String = {
    val valueMatch: Regex = """\/(?!.*\/).*\.parquet""".r

    valueMatch.findFirstMatchIn(valueString) match {
      case Some(_) => valueMatch.findFirstMatchIn(valueString).get.toString().substring(1).replace(".parquet", "")
      case None => "Unnamed"
    }
  }
  /**
   * Create metrics and write them to a file
   *
   * @param outputPath path to output folder
   * @param dataA A DataFrame whole data
   * @param dataB B DataFrame whole data
   * @param uniqA only unique rows in A DataFrame
   * @param uniqB only unique rows in B DataFrame
   */
  private def createMetrics(outputPath: String,
                            dataA: DataFrame, dataB: DataFrame,
                            uniqA: DataFrame, uniqB: DataFrame): Unit = {
    // compute metrics
    val rowCountA = dataA.count()
    val rowCountB = dataB.count()
    val sameRecords = if (rowCountA - uniqA.count() == rowCountB - uniqB.count()) rowCountA - uniqA.count() else -1

    val metricsJson =
      ("A" ->
        ("row count" -> rowCountA) ~
        ("column count" -> dataA.columns.length) ~
        ("diff column count" -> uniqA.count())) ~
      ("B" ->
          ("row count" -> rowCountB) ~
          ("column count" -> dataA.columns.length) ~
          ("diff column count" -> uniqB.count())) ~
      ("general" ->
          ("same records count" -> sameRecords) ~
          ("same records percent" -> (math floor (sameRecords.toFloat/rowCountA)*10000)/100))

    val jsonString = compact(render(metricsJson))
    // write metrics
    val outputFilePath = Paths.get(outputPath, "metrics.json")
    Files.write(outputFilePath, jsonString.getBytes)
  }

  /**
   * Compare two parquet files and write the differences and metrics to a folder
   *
   * @param dataA dataframe from input A
   * @param dataB dataframe from input B
   * @param outputPath to output folder
   * @param spark SparkSession
   */
  def compare(dataA: DataFrame, dataB: DataFrame, outputPath: String, spark: SparkSession): Unit = {

    // preprocess data todo will be solved by issue #5

    // compute hash rows
    val dfWithHashA: DataFrame = HashTable.hash(dataA)
    val dfWithHashB: DataFrame = HashTable.hash(dataB)

    // select non matching hashs
    val uniqHashA: DataFrame = dfWithHashA.select(Main.HashName).exceptAll(dfWithHashB.select(Main.HashName))
    val uniqHashB: DataFrame = dfWithHashB.select(Main.HashName).exceptAll(dfWithHashA.select(Main.HashName))

    // join on hash column (get back whole rows)
    val uniqA: DataFrame = dfWithHashA.join(uniqHashA, Seq(Main.HashName))
    val uniqB: DataFrame = dfWithHashB.join(uniqHashB, Seq(Main.HashName))

    // compute diff todo will be solved by issue #3

    // write unique rows to file
    SparkWriter.write(outputPath + "/Diff_" + extractName(filenameA) + "01", uniqA)
    SparkWriter.write(outputPath + "/Diff_" + extractName(filenameB) + "02", uniqB)

    // write diff todo will be solved by issue #3

    // create and write metrics
    createMetrics(outputPath, dataA, dataB, uniqA, uniqB)

    // show different rows
    println("Different rows: ")
    uniqA.show(numRows = 5, truncate = false)
    uniqB.show(numRows = 5, truncate = false)
  }

}
