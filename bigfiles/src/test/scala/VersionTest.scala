import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class VersionTest extends AnyFunSuite {
  test("test spark version") {
    val spark: SparkSession = SparkTestSession.spark
    assert(spark.version === "3.5.3")
  }
}
