package africa.absa.cps.writers

import org.apache.spark.sql.DataFrame

class HadoopWriter extends Writer {
  override def write(filePath: String, data: DataFrame): Unit = println(s"Writing $data to Hadoop") //todo: implement
}
