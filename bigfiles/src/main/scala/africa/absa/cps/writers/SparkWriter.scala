package africa.absa.cps.writers

import org.apache.spark.sql.{DataFrame, SaveMode}

object SparkWriter extends Writer {
  override def write(filePath: String, data: DataFrame): Unit = {
    println(s"Saving data ...")
    data.write.mode(SaveMode.Overwrite)
      .format("parquet")
      .save(filePath)
  }

}
