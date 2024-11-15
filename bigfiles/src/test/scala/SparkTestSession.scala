import org.apache.spark.sql.SparkSession

import java.io.File

object SparkTestSession {
  lazy val spark: SparkSession = SparkSession.builder()
    .appName("SparkTestSession")
    .config("spark.hadoop.fs.default.name", FS_URI)
    .config("spark.hadoop.fs.defaultFS", FS_URI)
    .master("local[*]")
    .getOrCreate()

  val FS_URI: String =  new File("src/test/resources").getAbsoluteFile.toURI.toString
}
