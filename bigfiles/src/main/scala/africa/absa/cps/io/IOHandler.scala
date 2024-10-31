package africa.absa.cps.io

import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.{Files, Paths}

object IOHandler{

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  /**
   * Read data from a file
   *
   * @param filePath path to file
   * @param spark SparkSession
   * @return DataFrame
   */
  def sparkRead(filePath: String)(implicit spark: SparkSession): DataFrame = {
    logger.info(s"Reading data from $filePath")
    spark.read.parquet(filePath)
  }

  /**
   * Write the data to a file
   *
   * @param filePath path to file
   * @param data DataFrame to write
   */
  def dfWrite(filePath: String, data: DataFrame): Unit = {
    logger.info(s"Saving data to $filePath")
    data.write.format("parquet").save(filePath)
  }

  /**
   * Write json data to a file
   *
   * @param filePath path to file
   * @param jsonString to write
   */
  def jsonWrite(filePath: String, jsonString: String)(implicit spark: SparkSession): Unit = {
    logger.info(s"Saving json data to $filePath")
    import spark.implicits._
    Seq(jsonString).toDS.write.json(filePath)
  }

}
