package africa.absa.cps.readers

import africa.absa.cps.models.RunData
import org.apache.spark.sql.{DataFrame, SparkSession}

object FileSystemReader extends Reader{

  override def read(filePath: String, spark: SparkSession): RunData[DataFrame] =  RunData(Array(spark.read.parquet(filePath)))
}
