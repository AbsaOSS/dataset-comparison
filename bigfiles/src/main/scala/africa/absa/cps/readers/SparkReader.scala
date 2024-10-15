package africa.absa.cps.readers

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkReader extends Reader{

  override def read(filePath: String, spark: SparkSession): DataFrame =
    spark.read.parquet(filePath)
}
