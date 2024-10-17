package africa.absa.cps.readers

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Reader {
  /**
   * Read data from a file
   *
   * @param filePath path to file
   * @param spark SparkSession
   * @return DataFrame
   */
  def read(filePath: String, spark: SparkSession): DataFrame

}
