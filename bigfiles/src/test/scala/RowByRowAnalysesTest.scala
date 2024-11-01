import africa.absa.cps.Comparator
import africa.absa.cps.analysis.RowByRowAnalysis.analyse
import africa.absa.cps.hash.HashUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite


class RowByRowAnalysesTest extends AnyFunSuite{
  test("test getBest") {
    implicit val spark: SparkSession = SparkTestSession.spark
    import spark.implicits._
    val dataA = Seq(
      (1, "a", 3.0),
      (4, "b", 4.0),
      (3, "c", 5.0)
    ).toDF("id", "name", "value")

    val dataB = Seq(
      (1, "b", 3.0),
      (2, "b", 4.5),
      (3, "c", 3.0)
    ).toDF("id", "name", "value")


    analyse(dataA, dataB)
  }
}


