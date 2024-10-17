package africa.absa.cps.writers

import org.apache.spark.sql.DataFrame

trait Writer {
  /**
   * Write the data to a file
   *
   * @param filePath path to file
   * @param data DataFrame to write
   */
  def write(filePath: String, data: DataFrame): Unit
}
