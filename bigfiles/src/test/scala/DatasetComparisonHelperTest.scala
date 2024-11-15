import africa.absa.cps.DatasetComparisonHelper
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class DatasetComparisonHelperTest extends AnyFunSuite {
  implicit val spark: SparkSession = SparkTestSession.spark

  import spark.implicits._

  test("test that exclude will exclude correct column"){
    val data: DataFrame = Seq((1, "one", "xx"), (2, "two", "xy"),
      (3, "two", "xy"), (4, "one", "xy")).toDF("id", "value", "code")
    val excluded: DataFrame = DatasetComparisonHelper.exclude(data, Seq("id"), "data")
    assert(excluded.columns.length == 2)
    assert(excluded.columns.contains("value"))
    assert(excluded.columns.contains("code"))

    assert(!excluded.columns.contains("id"))
  }

  test("test that exclude will exclude correct columns"){
    val data: DataFrame = Seq((1, "one", "xx"), (2, "two", "xy"),
      (3, "two", "xy"), (4, "one", "xy")).toDF("id", "value", "code")
    val excluded: DataFrame = DatasetComparisonHelper.exclude(data, Seq("id", "code"), "data")
    assert(excluded.columns.length == 1)
    assert(excluded.columns.contains("value"))

    assert(!excluded.columns.contains("code"))
    assert(!excluded.columns.contains("id"))
  }

  test("test exclude, exclude columns that are not present") {
    val data: DataFrame = Seq((1, "one", "xx"), (2, "two", "xy"),
      (3, "two", "xy"), (4, "one", "xy")).toDF("id", "value", "code")
    val excluded: DataFrame = DatasetComparisonHelper.exclude(data, Seq("type", "name"), "data")
    assert(excluded.columns.length == 3)
    assert(excluded.columns.contains("value"))
    assert(excluded.columns.contains("code"))
    assert(excluded.columns.contains("id"))

  }

  test("test exclude, exclude contains both columns that are not present and columns that are present") {
    val data: DataFrame = Seq((1, "one", "xx"), (2, "two", "xy"),
      (3, "two", "xy"), (4, "one", "xy")).toDF("id", "value", "code")
    val excluded: DataFrame = DatasetComparisonHelper.exclude(data, Seq("type", "id", "name", "value"), "data")
    assert(excluded.columns.length == 1)
    assert(excluded.columns.contains("code"))

    assert(!excluded.columns.contains("value"))
    assert(!excluded.columns.contains("id"))
  }


  test("test exclude with empty DataFrame") {
    val data: DataFrame = spark.emptyDataFrame
    assert(data.columns.length == 0)
    val excluded: DataFrame = DatasetComparisonHelper.exclude(data, Seq("type", "id", "name", "value"), "data")
    assert(excluded.columns.length == 0)
  }
  test("test exclude with empty to exclude columns") {
    val data: DataFrame = Seq((1, "one", "xx"), (2, "two", "xy"),
      (3, "two", "xy"), (4, "one", "xy")).toDF("id", "value", "code")
    val excluded: DataFrame = DatasetComparisonHelper.exclude(data, Seq(), "data")
    assert(excluded.columns.length == 3)
    assert(excluded.columns.contains("value"))
    assert(excluded.columns.contains("code"))
    assert(excluded.columns.contains("id"))
  }
}
