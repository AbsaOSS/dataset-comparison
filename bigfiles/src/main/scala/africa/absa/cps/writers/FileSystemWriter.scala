package africa.absa.cps.writers

import africa.absa.cps.models.RunData
import org.apache.spark.sql.{DataFrame, SaveMode}

object FileSystemWriter extends Writer {
  override def write(filePath: String, data: DataFrame): Unit = {
    println(s"Writing data to file system")
    data.coalesce(1).write.mode(SaveMode.Overwrite)
      .format("parquet")
      .save("src/main/resources/saved/"+filePath)
  }

}
