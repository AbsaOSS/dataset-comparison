package africa.absa.cps.writers

import org.apache.spark.sql.{DataFrame, SaveMode}

import org.slf4j.Logger
import org.slf4j.LoggerFactory

object SparkWriter{

  private val logger: Logger = LoggerFactory.getLogger(SparkWriter.getClass)

  /**
   * Write the data to a file
   *
   * @param filePath path to file
   * @param data DataFrame to write
   */
   def write(filePath: String, data: DataFrame): Unit = {
    logger.info(s"Saving data to $filePath")
    data.write.mode(SaveMode.Overwrite)
      .format("parquet")
      .save(filePath)
  }

}
