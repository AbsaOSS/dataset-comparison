package africa.absa.cps.readers

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkReader{
  /**
   * Read data from a file
   *
   * @param filePath path to file
   * @param spark SparkSession
   * @return DataFrame
   */
  def read(filePath: String, spark: SparkSession): DataFrame =
    spark.read.parquet(filePath)
}
