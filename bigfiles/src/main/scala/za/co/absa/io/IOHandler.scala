/** Copyright 2020 ABSA Group Limited
  *
  * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
  * the License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */

package za.co.absa.io

import za.co.absa.analysis.{ColumnsDiff, RowsDiff}
import za.co.absa.parser.OutputFormatType
import za.co.absa.parser.OutputFormatType._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import java.io.ByteArrayInputStream
import java.nio.file.{Files, Paths}

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
  def dfWrite(filePath: String, data: DataFrame, format: OutputFormatType): Unit = {
    logger.info(s"Saving data to $filePath")
    var writer = data.write.format(format.toString)
    if (format == OutputFormatType.CSV) {
      writer = data.write.format(format.toString)
      writer.option("header", "true")
    }
    writer.save(filePath)
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

  /** Write the row diff to a file as JSON
    * @param filePath
    *   path to write the diff
    * @param diff
    *   list of row differences
    *
    * ColumnsDiffRw are required for upickle, when write is used on List[RowsDiff] it will call write on ColumnsDiff
    */
  def rowDiffWriteAsJson(filePath: String, diff: Seq[RowsDiff])(implicit spark: SparkSession): Unit = {
    logger.info(s"Saving row diff to $filePath")
    import upickle.default._
    implicit val ColumnsDiffRw: ReadWriter[ColumnsDiff] = macroRW // Required for upickle
    implicit val RowDiffRw: ReadWriter[RowsDiff]        = macroRW
    val diffJson                                        = write(diff, indent = 4)
    Files.write(Paths.get(filePath), diffJson.getBytes)
    logger.info(s"Saved to $filePath")
  }

}
