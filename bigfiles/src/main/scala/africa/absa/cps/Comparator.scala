package africa.absa.cps

import hash.HashTable
import readers.SparkReader
import writers.SparkWriter
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.native.JsonMethods.{compact, render}

import java.nio.file.{Files, Paths}
import scala.util.matching.Regex

import org.json4s.JsonDSL._


object Comparator {
  private def extractName(valueString: String): String = {
    val valueMatch: Regex = """\/(?!.*\/).*\.parquet""".r

    valueMatch.findFirstMatchIn(valueString) match {
      case Some(_) => valueMatch.findFirstMatchIn(valueString).get.toString().substring(1).replace(".parquet", "")
      case None => "Unnamed"
    }
  }
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

  def compare(oldFilename: String, newFilename: String, outputPath: String, spark: SparkSession): Unit = {
    // read files
    val oldData: DataFrame = SparkReader.read(oldFilename, spark)
    val newData: DataFrame = SparkReader.read(newFilename, spark)

    // preprocess data todo

    // compute hash rows
    val oldWithHash: DataFrame = HashTable.hash(oldData)
    val newWithHash: DataFrame = HashTable.hash(newData)

    // select non matching hashs
    val oldUniqHash: DataFrame = oldWithHash.select("hash").exceptAll(newWithHash.select("hash"))
    val newUniqHash: DataFrame = newWithHash.select("hash").exceptAll(oldWithHash.select("hash"))

    // join on hash column (get back whole rows)
    val oldUniq: DataFrame = oldWithHash.join(oldUniqHash, Seq("hash"))
    val newUniq: DataFrame = newWithHash.join(newUniqHash, Seq("hash"))

    // compute diff todo

    // write unique rows to file
    SparkWriter.write(outputPath + "/Diff_" + extractName(oldFilename), oldUniq)
    SparkWriter.write(outputPath + "/Diff_" + extractName(newFilename), newUniq)

    // write diff todo

    // create and write metrics
    createMetrics(outputPath, oldData, newData, oldUniq, newUniq)

    // show different rows
    println("Different rows: ")
    oldUniq.show(numRows = 5, truncate = false)
    newUniq.show(numRows = 5, truncate = false)
  }

}
