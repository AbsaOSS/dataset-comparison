package africa.absa.cps.io

import africa.absa.cps.analysis.{ColumnsDiff, RowsDiff}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import java.io.ByteArrayInputStream

object IOHandler {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /** Read data from a file
    *
    * @param filePath
    *   path to file
    * @param spark
    *   SparkSession
    * @return
    *   DataFrame
    */
  def sparkRead(filePath: String)(implicit spark: SparkSession): DataFrame = {
    logger.info(s"Reading data from $filePath")
    spark.read.parquet(filePath)
  }

  /** Write the data to a file
    *
    * @param filePath
    *   path to file
    * @param data
    *   DataFrame to write
    */
  def dfWrite(filePath: String, data: DataFrame): Unit = {
    logger.info(s"Saving data to $filePath")
    data.write.format("parquet").save(filePath)
  }

  /** Write json data to a file
    *
    * @param filePath
    *   path to file
    * @param jsonString
    *   to write
    */
  def jsonWrite(filePath: String, jsonString: String)(implicit spark: SparkSession): Unit = {
    val config = spark.sparkContext.hadoopConfiguration
    val fs     = FileSystem.get(config)
    logger.info(s"Saving json data to $filePath")
    // save the json string to a file
    val path         = new Path(filePath)
    val outputStream = fs.create(path)
    val inputStream  = new ByteArrayInputStream(jsonString.getBytes("UTF-8"))
    try {
      IOUtils.copyBytes(inputStream, outputStream, config, true)
    } finally {
      IOUtils.closeStream(inputStream)
      IOUtils.closeStream(outputStream)
    }
  }

  /**
   * Write the row diff to a file as JSON
   * @param filePath path to write the diff
   * @param diff list of row differences
   *
   * ColumnsDiffRw are required for upickle, when write is used on List[RowsDiff] it will call write on ColumnsDiff
   */
  def rowDiffWriteAsJson(filePath: String, diff: List[RowsDiff])(implicit spark: SparkSession): Unit = {
    logger.info(s"Saving row diff to $filePath")
    import upickle.default._
    implicit val ColumnsDiffRw: ReadWriter[ColumnsDiff] = macroRW // Required for upickle
    implicit val RowDiffRw: ReadWriter[RowsDiff] = macroRW
    val diffJson = write(diff, indent = 4)
    jsonWrite(filePath, diffJson)
  }

}
