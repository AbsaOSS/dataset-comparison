import africa.absa.cps.Comparator
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class ComparatorTest extends AnyFunSuite with BeforeAndAfterAll {
  implicit val spark: SparkSession = SparkTestSession.spark

  import spark.implicits._

  test("test that comparator returns diff rows"){
    val tmp1: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")
    val tmp2: DataFrame = Seq((1, "one"), (2, "three"), (3, "two")).toDF("id", "value")

    val (diff1, diff2) = Comparator.compare(tmp1, tmp2)
    assert(diff1.count() == 1)
    assert(diff2.count() == 2)
  }

  test("test that comparator returns all rows if dataframes are completely different"){
    val tmp1: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")
    val tmp2: DataFrame = Seq(("12af", 1003), ("12qw", 3004), ("123q", 3456)).toDF("id", "value")

    val (diff1, diff2) = Comparator.compare(tmp1, tmp2)
    assert(diff1.count() == 2)
    assert(diff2.count() == 3)
  }

  test("test that comparator returns empty DataFrames if all rows are the same"){
    val tmp1: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")
    val tmp2: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")

    val (diff1, diff2) = Comparator.compare(tmp1, tmp2)
    assert(diff1.count() == 0)
    assert(diff2.count() == 0)
  }

  test("test that comparator returns empty DataFrames if all rows are the same but in different order"){
    val tmp1: DataFrame = Seq((2, "two"), (1, "one")).toDF("id", "value")
    val tmp2: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")

    val (diff1, diff2) = Comparator.compare(tmp1, tmp2)
    assert(diff1.count() == 0)
    assert(diff2.count() == 0)
  }

  test("test that comparator returns correct dataframes if one duplicate is present in one table"){
    val tmp1: DataFrame = Seq((1, "one"), (1, "one"), (2, "two")).toDF("id", "value")
    val tmp2: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")

    val (diff1, diff2) = Comparator.compare(tmp1, tmp2)

    assert(diff1.count() == 1)
    assert(diff2.count() == 0)
  }

  test("test that comparator returns correct dataframes if 2 duplicates are present in one table"){
    val tmp1: DataFrame = Seq((1, "one"), (1, "one"), (1, "one"), (2, "two")).toDF("id", "value")
    val tmp2: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")

    val (diff1, diff2) = Comparator.compare(tmp1, tmp2)

    assert(diff1.count() == 2)
    assert(diff2.count() == 0)
  }


  test("test that comparator returns correct dataframes if duplicates are present"){
    val tmp1: DataFrame = Seq((1, "one"), (1, "one"), (2, "two")).toDF("id", "value")
    val tmp2: DataFrame = Seq((1, "one"), (1, "one"), (1, "one"), (2, "two")).toDF("id", "value")
    val (diff1, diff2) = Comparator.compare(tmp1, tmp2)

    assert(diff1.count() == 0)
    assert(diff2.count() == 1)
  }


  //////////////////////////// createMetrics /////////////////////////////////////

  test("test that createMetrics returns correct JSON string"){
    val tmp1: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")
    val tmp2: DataFrame = Seq((1, "one"), (2, "three"), (3, "two")).toDF("id", "value")
    val (diff1, diff2) = Comparator.compare(tmp1, tmp2)
    val metrics = Comparator.createMetrics(tmp1, tmp2, diff1, diff2)
    val expected = "{\"A\":{\"row count\":2," +
                      "\"column count\":2," +
                      "\"rows not present in B\":1}," +
                      "\"unique rows count\":2," +
                    "\"B\":{\"row count\":3," +
                      "\"column count\":2," +
                      "\"rows not present in A\":2}," +
                      "\"unique rows count\":3," +
                    "\"general\":{\"same records count\":1,\"same records percent to A\":50.0}}"
    assert(metrics == expected)
  }

  test("test that createMetrics returns correct JSON string, duplicates in data"){
    val tmp1: DataFrame = Seq((1, "one"), (1, "one"), (2, "two")).toDF("id", "value")
    val tmp2: DataFrame = Seq((1, "one"), (1, "one"), (1, "one"), (2, "two")).toDF("id", "value")
    val (diff1, diff2) = Comparator.compare(tmp1, tmp2)
    val metrics = Comparator.createMetrics(tmp1, tmp2, diff1, diff2)
    val expected = "{\"A\":{\"row count\":3," +
      "\"column count\":2," +
      "\"rows not present in B\":0}," +
      "\"unique rows count\":2," +
      "\"B\":{\"row count\":4," +
      "\"column count\":2," +
      "\"rows not present in A\":1}," +
      "\"unique rows count\":2," +
      "\"general\":{\"same records count\":3,\"same records percent to A\":100.0}}"
    assert(metrics == expected)
  }
}
