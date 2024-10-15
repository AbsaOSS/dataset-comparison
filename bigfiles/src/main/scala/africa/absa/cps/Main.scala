package africa.absa.cps

import org.apache.spark.sql.SparkSession
import hash.HashTable
import readers.SparkReader
import africa.absa.cps.writers.SparkWriter

import scala.util.matching.Regex

object Main {
  def main(args: Array[String]): Unit = {
    val (oldFilename, newFilename, outputPath) = readArgs(args)

    val spark = SparkSession.builder()
      .appName("SimpleSparkApp")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    compare(oldFilename, newFilename, outputPath, spark)
  }

  private def readArgs(args: Array[String]): (String, String, String) = {
    if (args.length < 3) {
      println("Usage: App <old_parquet_path> <new_parquet_path> <output_path>")
      System.exit(1)
    }
    (args(0),  args(1), args(2))
  }
  private def extract(valueString: String): String = {
    val valueMatch: Regex = """\/(?!.*\/).*\.parquet""".r

    valueMatch.findFirstMatchIn(valueString) match {
      case Some(_) => valueMatch.findFirstMatchIn(valueString).get.toString().substring(1).replace(".parquet", "")
      case None => "Unnamed"
    }
  }

  private def compare(oldFilename: String, newFilename: String, outputPath: String, spark: SparkSession): Unit = {
    // read files
    val dataOld = SparkReader.read(oldFilename, spark)
    val dataNew = SparkReader.read(newFilename, spark)

    // compute hash rows
    val oldWithHash = HashTable.hash(dataOld)
    val newWithHash = HashTable.hash(dataNew)

    // select non matching hashs
    val oldUniqHash = oldWithHash.select("hash").exceptAll(newWithHash.select("hash"))
    val newUniqHash = newWithHash.select("hash").exceptAll(oldWithHash.select("hash"))

    // join on hash column (get back whole rows)
    val oldUniq = oldWithHash.join(oldUniqHash, Seq("hash"))
    val newUniq = newWithHash.join(newUniqHash, Seq("hash"))

    // write to file
    SparkWriter.write(outputPath + "/Diff_" + extract(oldFilename), oldUniq)
    SparkWriter.write(outputPath + "/Diff_" + extract(newFilename), newUniq)

    // show different rows
    println("Different rows: ")
    oldUniq.show()
    newUniq.show()
  }

}

