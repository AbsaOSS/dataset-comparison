package africa.absa.cps

import africa.absa.cps.parser.ArgsParser
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val (oldFilename, newFilename, outputPath) = readArgs(args)

    val spark = SparkSession.builder()
      .appName("DatasetComparator")
      .getOrCreate()

    import spark.implicits._

    Comparator.compare(oldFilename, newFilename, outputPath, spark)
  }

  /**
   * Read the arguments from the command line by using the ArgsParser
   * @param args arguments from the command line
   * @return a tuple containing the old file path, the new file path and the output path
   */
  private def readArgs(args: Array[String]): (String, String, String) = {
    val result = ArgsParser.getArgs(args)
    if (result.isEmpty) {
      System.exit(1)
    }
    result.get

  }
}

