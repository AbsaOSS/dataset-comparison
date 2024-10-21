package africa.absa.cps.readers

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object SparkReader{

  private val logger: Logger = LoggerFactory.getLogger(SparkReader.getClass)
  /**
   * Read data from a file
   *
   * @param filePath path to file
   * @param spark SparkSession
   * @return DataFrame
   */
  def read(filePath: String, spark: SparkSession): DataFrame = {
    logger.info(s"Reading data from $filePath")
    spark.read.parquet(filePath)
  }

}
