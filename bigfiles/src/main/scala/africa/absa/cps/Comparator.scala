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
   * @param oldData old DataFrame whole data
   * @param newData new DataFrame whole data
   * @param oldUniq only unique rows in old DataFrame
   * @param newUniq only unique rows in new DataFrame
   */
  private def createMetrics(outputPath: String,
                            oldData: DataFrame, newData: DataFrame,
                            oldUniq: DataFrame, newUniq: DataFrame): Unit = {
    // compute metrics
    val oldRowCount = oldData.count()
    val newRowCount = newData.count()
    val sameRecords = if (oldRowCount - oldUniq.count() == newRowCount - newUniq.count()) oldRowCount - oldUniq.count() else -1

    val metricsJson =
      ("old" ->
        ("row count" -> oldRowCount) ~
        ("column count" -> oldData.columns.length) ~
        ("diff column count" -> oldUniq.count())) ~
      ("new" ->
          ("row count" -> newRowCount) ~
          ("column count" -> oldData.columns.length) ~
          ("diff column count" -> newUniq.count())) ~
      ("general" ->
          ("same records count" -> sameRecords) ~
          ("same records percent" -> (math floor (sameRecords.toFloat/oldRowCount)*10000)/100))

    val jsonString = compact(render(metricsJson))
    // write metrics
    val outputFilePath = Paths.get(outputPath, "metrics.json")
    Files.write(outputFilePath, jsonString.getBytes)
  }

  /**
   * Compare two parquet files and write the differences and metrics to a folder
   *
   * @param oldFilename path to old parquet file to compare
   * @param newFilename path to new parquet file to compare
   * @param outputPath to output folder
   * @param spark SparkSession
   */
  def compare(oldFilename: String, newFilename: String, outputPath: String, spark: SparkSession): Unit = {
    // read files
    val oldData: DataFrame = SparkReader.read(oldFilename, spark)
    val newData: DataFrame = SparkReader.read(newFilename, spark)

    // preprocess data todo will be solved by issue #5

    // compute hash rows
    val oldWithHash: DataFrame = HashTable.hash(oldData)
    val newWithHash: DataFrame = HashTable.hash(newData)

    // select non matching hashs
    val oldUniqHash: DataFrame = oldWithHash.select("hash").exceptAll(newWithHash.select("hash"))
    val newUniqHash: DataFrame = newWithHash.select("hash").exceptAll(oldWithHash.select("hash"))

    // join on hash column (get back whole rows)
    val oldUniq: DataFrame = oldWithHash.join(oldUniqHash, Seq("hash"))
    val newUniq: DataFrame = newWithHash.join(newUniqHash, Seq("hash"))

    // compute diff todo will be solved by issue #3

    // write unique rows to file
    SparkWriter.write(outputPath + "/Diff_" + extractName(oldFilename) + "01", oldUniq)
    SparkWriter.write(outputPath + "/Diff_" + extractName(newFilename) + "02", newUniq)

    // write diff todo will be solved by issue #3

    // create and write metrics
    createMetrics(outputPath, oldData, newData, oldUniq, newUniq)

    // show different rows
    println("Different rows: ")
    oldUniq.show(numRows = 5, truncate = false)
    newUniq.show(numRows = 5, truncate = false)
  }

}
