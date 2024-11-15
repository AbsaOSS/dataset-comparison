
import africa.absa.cps.analysis.RowByRowAnalysis.analyse
import africa.absa.cps.hash.HashUtils.HASH_COLUMN_NAME
import org.apache.spark.sql.SparkSession
import org.json4s.native.JsonMethods.{compact, parse, render}
import org.scalatest.funsuite.AnyFunSuite


class RowByRowAnalysesTest extends AnyFunSuite{
  implicit val spark: SparkSession = SparkTestSession.spark
  import spark.implicits._
//  spark.sparkContext.setLogLevel("DEBUG")

  test("test analyses multiple changes") {
    val dataA = Seq(
      (1, "a", 3.0, "001a010"),
      (4, "b", 4.0, "100b100"),
      (3, "c", 5.0, "011c101")
    ).toDF("id", "name", "value", HASH_COLUMN_NAME)

    val dataB = Seq(
      (1, "b", 3.0, "001b010"),
      (2, "b", 4.5, "010b100"),
      (3, "c", 3.0, "011c011")
    ).toDF("id", "name", "value", HASH_COLUMN_NAME)


    val diff = analyse(dataA, dataB)
    val jsonStringA = compact(render(parse(diff._1)))
    assert(jsonStringA.contains("\"001a010 001b010\":{\"name\":[\"a\",\"b\"]}"))
    assert(jsonStringA.contains("\"100b100 010b100\":{\"id\":[\"4\",\"2\"],\"value\":[\"4.0\",\"4.5\"]}") || jsonStringA.contains("\"100b100 001b010\":{\"id\":[\"4\",\"1\"],\"value\":[\"4.0\",\"3.0\"]}"))
    assert(jsonStringA.contains("\"011c101 011c011\":{\"value\":[\"5.0\",\"3.0\"]}"))

    val jsonStringB = compact(render(parse(diff._2)))
    assert(jsonStringB.contains("\"001b010 001a010\":{\"name\":[\"b\",\"a\"]}"))
    assert(jsonStringB.contains("\"010b100 100b100\":{\"id\":[\"2\",\"4\"],\"value\":[\"4.5\",\"4.0\"]}"))
    assert(jsonStringB.contains("\"011c011 011c101\":{\"value\":[\"3.0\",\"5.0\"]}"))


  }
  test("test analyses change in the same column") {
    val dataA = Seq(
      (1, "a", 3.0, "001a010"),
      (4, "b", 4.0, "100b100"),
      (3, "c", 5.0, "011c101")
    ).toDF("id", "name", "value", HASH_COLUMN_NAME)

    val dataB = Seq(
      (1, "a", 3.5, "001b010"),
      (4, "b", 4.5, "010b100"),
      (3, "c", 5.5, "011c011")
    ).toDF("id", "name", "value", HASH_COLUMN_NAME)

    val diff = analyse(dataA, dataB)
    val jsonStringA = compact(render(parse(diff._1)))
    assert(jsonStringA.contains("\"001a010 001b010\":{\"value\":[\"3.0\",\"3.5\"]}"))
    assert(jsonStringA.contains("\"100b100 010b100\":{\"value\":[\"4.0\",\"4.5\"]}"))
    assert(jsonStringA.contains("\"011c101 011c011\":{\"value\":[\"5.0\",\"5.5\"]}"))

    val jsonStringB = compact(render(parse(diff._2)))
    assert(jsonStringB.contains("\"001b010 001a010\":{\"value\":[\"3.5\",\"3.0\"]}"))
    assert(jsonStringB.contains("\"010b100 100b100\":{\"value\":[\"4.5\",\"4.0\"]}"))
    assert(jsonStringB.contains("\"011c011 011c101\":{\"value\":[\"5.5\",\"5.0\"]}"))
  }

  test("test analyses one change in different columns"){
    val dataA = Seq(
      (1, "a", 3.0, "001a010"),
      (4, "b", 4.0, "100b100"),
      (3, "c", 5.0, "011c101")
    ).toDF("id", "name", "value", HASH_COLUMN_NAME)

    val dataB = Seq(
      (1, "b", 3.0, "001b010"),
      (4, "b", 4.5, "010b100"),
      (4, "c", 5.0, "011c011")
    ).toDF("id", "name", "value", HASH_COLUMN_NAME)


    val diff = analyse(dataA, dataB)
    val jsonStringA = compact(render(parse(diff._1)))
    assert(jsonStringA.contains("\"001a010 001b010\":{\"name\":[\"a\",\"b\"]}"))
    assert(jsonStringA.contains("\"100b100 010b100\":{\"value\":[\"4.0\",\"4.5\"]}"))
    assert(jsonStringA.contains("\"011c101 011c011\":{\"id\":[\"3\",\"4\"]}"))

    val jsonStringB = compact(render(parse(diff._2)))
    assert(jsonStringB.contains("\"001b010 001a010\":{\"name\":[\"b\",\"a\"]}"))
    assert(jsonStringB.contains("\"010b100 100b100\":{\"value\":[\"4.5\",\"4.0\"]}"))
    assert(jsonStringB.contains("\"011c011 011c101\":{\"id\":[\"4\",\"3\"]}"))
  }
}


