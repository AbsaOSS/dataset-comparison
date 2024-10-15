package africa.absa.cps.readers

import africa.absa.cps.models.RunData
import africa.absa.cps.readers.{Reader => BaseReader}
import africa.absa.cps.readers.Reader
import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}

object AWSReader extends Reader {
  override def read(filePath: String, spark: SparkSession): RunData[DataFrame] = {
//    val df: DataFrame = spark.read.parquet(filePath)
//    RunData(Array(df))
    println(s"Reading data from AWS") //todo implement
    RunData(Array())
  }

}
