package africa.absa.cps

import org.apache.spark.sql.{DataFrame, SparkSession}


object Main {
  def main(args: Array[String]): Unit = {
    val (oldFilename, newFilename, outputPath) = readArgs(args)

    val spark = SparkSession.builder()
      .appName("DatasetComparator")
      .getOrCreate()

    import spark.implicits._

    Comparator.compare(oldFilename, newFilename, outputPath, spark)
  }

  private def readArgs(args: Array[String]): (String, String, String) = {
    if (args.length < 3) {
      println("Usage: App <old_parquet_path> <new_parquet_path> <output_path>")
      System.exit(1)
    }
    (args(0),  args(1), args(2))
  }

}

