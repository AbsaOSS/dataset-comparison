package africa.absa.cps

import org.apache.spark.sql.SparkSession
import hash.HashTable
import readers.FileSystemReader
import africa.absa.cps.writers.FileSystemWriter

import scala.util.matching.Regex

object Main {
  def main(args: Array[String]): Unit = {
    val (oldFilename, newFilename) = readArgs(args)

    val spark = SparkSession.builder()
      .appName("SimpleSparkApp")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    compare(oldFilename, newFilename, spark)
  }

  private def readArgs(args: Array[String]): (String, String) = {
    if (args.length < 2) {
      println("Usage: App <old_parquet_file> <new_parquet_file>")
      System.exit(1)
    }
    (args(0),  args(1))
  }
  private def extract(valueString: String): String = {
    val valueMatch: Regex = """\/(?!.*\/).*\.parquet""".r

    valueMatch.findFirstMatchIn(valueString) match {
      case Some(_) => valueMatch.findFirstMatchIn(valueString).get.toString().substring(1).replace(".parquet", "")
      case None => "Unnamed"
    }
  }

  private def compare(oldFilename: String, newFilename: String, spark: SparkSession): Unit = {
    // todo more then one file
    // read files
    val dataOld = FileSystemReader.read(oldFilename, spark).files(0)
    val dataNew = FileSystemReader.read(newFilename, spark).files(0)

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
    FileSystemWriter.write(extract(oldFilename), oldUniq)
    FileSystemWriter.write(extract(newFilename), newUniq)

    // show different rows
    println("Different rows: ")
    oldUniq.show()
    newUniq.show()
  }

}

