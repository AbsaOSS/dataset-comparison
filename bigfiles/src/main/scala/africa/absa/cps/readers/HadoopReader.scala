package africa.absa.cps.readers

import africa.absa.cps.models.RunData
import africa.absa.cps.readers.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}

object HadoopReader extends Reader {
  override def read(filePath: String, spark: SparkSession): RunData[DataFrame] = {
    println(s"Reading data from Hadoop") //todo implement
    RunData(Array())
  }
}
