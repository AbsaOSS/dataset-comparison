package africa.absa.cps.readers

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Reader {
  def read(filePath: String, spark: SparkSession): DataFrame

}
