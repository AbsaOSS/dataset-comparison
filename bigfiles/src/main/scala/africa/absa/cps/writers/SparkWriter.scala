package africa.absa.cps.writers

import org.apache.spark.sql.{DataFrame, SaveMode}

object SparkWriter{

  /**
   * Write the data to a file
   *
   * @param filePath path to file
   * @param data DataFrame to write
   */
   def write(filePath: String, data: DataFrame): Unit = {
    println(s"Saving data ...")
    data.write.mode(SaveMode.Overwrite)
      .format("parquet")
      .save(filePath)
  }

}
