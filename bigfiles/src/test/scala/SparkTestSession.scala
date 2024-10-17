import org.apache.spark.sql.SparkSession

object SparkTestSession {
  lazy val spark: SparkSession = SparkSession.builder()
    .appName("SparkTestSession")
    .master("local[*]")
    .getOrCreate()
}
