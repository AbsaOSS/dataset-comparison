package africa.absa.cps.writers

import org.apache.spark.sql.DataFrame

trait Writer {
  def write(filePath: String, data: DataFrame): Unit
}
