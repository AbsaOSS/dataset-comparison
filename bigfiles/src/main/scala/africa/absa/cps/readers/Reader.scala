package africa.absa.cps.readers

import africa.absa.cps.models.RunData
import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Reader {
  def read(filePath: String, spark: SparkSession): RunData[DataFrame]

}
