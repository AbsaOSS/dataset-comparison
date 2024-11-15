
import africa.absa.cps.analysis.RowByRowAnalysis.analyse
import africa.absa.cps.hash.HashUtils.HASH_COLUMN_NAME
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.json4s.native.JsonMethods.{compact, parse, render}
import org.scalatest.funsuite.AnyFunSuite


class RowByRowAnalysesTest extends AnyFunSuite{
  implicit val spark: SparkSession = SparkTestSession.spark
  import spark.implicits._
//  spark.sparkContext.setLogLevel("DEBUG")

  test("test analyses multiple changes") {
    val dataA = Seq(
      (1, "a", 3.0, 11133),
      (4, "b", 4.0, 49840),
      (3, "c", 5.0, 39950)
    ).toDF("id", "name", "value", HASH_COLUMN_NAME)

    val dataB = Seq(
      (1, "b", 3.0, 19830),
      (2, "b", 4.5, 29845),
      (3, "c", 3.0, 39955)
    ).toDF("id", "name", "value", HASH_COLUMN_NAME)


    val diff = analyse(dataA, dataB)
    val jsonStringA = compact(render(parse(diff._1)))
    assert(jsonStringA.contains("\"11133 19830\":{\"name\":[\"a\",\"b\"]}"))
    assert(jsonStringA.contains("\"49840 29845\":{\"id\":[\"4\",\"2\"],\"value\":[\"4.0\",\"4.5\"]}") || jsonStringA.contains("\"49840 19830\":{\"id\":[\"4\",\"1\"],\"value\":[\"4.0\",\"3.0\"]}"))
    assert(jsonStringA.contains("\"39950 39955\":{\"value\":[\"5.0\",\"3.0\"]}"))

    val jsonStringB = compact(render(parse(diff._2)))
    assert(jsonStringB.contains("\"19830 11133\":{\"name\":[\"b\",\"a\"]}"))
    assert(jsonStringB.contains("\"29845 49840\":{\"id\":[\"2\",\"4\"],\"value\":[\"4.5\",\"4.0\"]}"))
    assert(jsonStringB.contains("\"39955 39950\":{\"value\":[\"3.0\",\"5.0\"]}"))


  }
  test("test analyses change in the same column") {
    val dataA = Seq(
      (1, "a", 3.0, 11133),
      (4, "b", 4.0, 49840),
      (3, "c", 5.0, 39950)
    ).toDF("id", "name", "value", HASH_COLUMN_NAME)

    val dataB = Seq(
      (1, "a", 3.5, 19830),
      (4, "b", 4.5, 29845),
      (3, "c", 5.5, 39955)
    ).toDF("id", "name", "value", HASH_COLUMN_NAME)

    val diff = analyse(dataA, dataB)
    val jsonStringA = compact(render(parse(diff._1)))
    assert(jsonStringA.contains("\"11133 19830\":{\"value\":[\"3.0\",\"3.5\"]}"))
    assert(jsonStringA.contains("\"49840 29845\":{\"value\":[\"4.0\",\"4.5\"]}"))
    assert(jsonStringA.contains("\"39950 39955\":{\"value\":[\"5.0\",\"5.5\"]}"))

    val jsonStringB = compact(render(parse(diff._2)))
    assert(jsonStringB.contains("\"19830 11133\":{\"value\":[\"3.5\",\"3.0\"]}"))
    assert(jsonStringB.contains("\"29845 49840\":{\"value\":[\"4.5\",\"4.0\"]}"))
    assert(jsonStringB.contains("\"39955 39950\":{\"value\":[\"5.5\",\"5.0\"]}"))
  }

  test("test analyses one change in different columns"){
    val dataA = Seq(
      (1, "a", 3.0, 11133),
      (4, "b", 4.0, 49840),
      (3, "c", 5.0, 39950)
    ).toDF("id", "name", "value", HASH_COLUMN_NAME)

    val dataB = Seq(
      (1, "b", 3.0, 19830),
      (4, "b", 4.5, 29845),
      (4, "c", 5.0, 39955)
    ).toDF("id", "name", "value", HASH_COLUMN_NAME)


    val diff = analyse(dataA, dataB)
    val jsonStringA = compact(render(parse(diff._1)))
    assert(jsonStringA.contains("\"11133 19830\":{\"name\":[\"a\",\"b\"]}"))
    assert(jsonStringA.contains("\"49840 29845\":{\"value\":[\"4.0\",\"4.5\"]}"))
    assert(jsonStringA.contains("\"39950 39955\":{\"id\":[\"3\",\"4\"]}"))

    val jsonStringB = compact(render(parse(diff._2)))
    assert(jsonStringB.contains("\"19830 11133\":{\"name\":[\"b\",\"a\"]}"))
    assert(jsonStringB.contains("\"29845 49840\":{\"value\":[\"4.5\",\"4.0\"]}"))
    assert(jsonStringB.contains("\"39955 39950\":{\"id\":[\"4\",\"3\"]}"))
  }

  test("test analyses one DataFrame is empty"){
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("value", DoubleType, nullable = true),
      StructField(HASH_COLUMN_NAME, StringType, nullable = true)
    ))

    val dataA = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    val dataB = Seq(
      (1, "b", 3.0, 19830),
      (4, "b", 4.5, 29845),
      (4, "c", 5.0, 39955)
    ).toDF("id", "name", "value", HASH_COLUMN_NAME)


    val diff = analyse(dataA, dataB)
    val jsonStringA = compact(render(parse(diff._1)))
    assert(jsonStringA == "{\"result\":\"All rows matched\"}")

    val jsonStringB = compact(render(parse(diff._2)))
    assert(jsonStringB == "{\"result\":\"There is no other row in A look to inputB_differences\"}")

    val diffReverse = analyse(dataB, dataA)
    val jsonStringBRe = compact(render(parse(diffReverse._1)))
    assert(jsonStringBRe == "{\"result\":\"There is no other row in B look to inputA_differences\"}")

    val jsonStringARe = compact(render(parse(diffReverse._2)))
    assert(jsonStringARe == "{\"result\":\"All rows matched\"}")


  }

  test("test analyses one Dataframe is smaller than the other"){
    val dataA = Seq(
      (1, "a", 3.0, 11133),
      (4, "b", 4.0, 49840),
    ).toDF("id", "name", "value", HASH_COLUMN_NAME)

    val dataB = Seq(
      (1, "a", 3.5, 19830),
      (4, "b", 4.5, 29845),
      (3, "c", 5.5, 39955)
    ).toDF("id", "name", "value", HASH_COLUMN_NAME)

    val diff = analyse(dataA, dataB)
    val jsonStringA = compact(render(parse(diff._1)))
    assert(jsonStringA.contains("\"11133 19830\":{\"value\":[\"3.0\",\"3.5\"]}"))
    assert(jsonStringA.contains("\"49840 29845\":{\"value\":[\"4.0\",\"4.5\"]}"))

    val jsonStringB = compact(render(parse(diff._2)))
    assert(jsonStringB.contains("\"19830 11133\":{\"value\":[\"3.5\",\"3.0\"]}"))
    assert(jsonStringB.contains("\"29845 49840\":{\"value\":[\"4.5\",\"4.0\"]}"))
    assert(jsonStringB.contains("\"39955 11133\":{\"id\":[\"3\",\"1\"],\"name\":[\"c\",\"a\"],\"value\":[\"5.5\",\"3.0\"]}") ||
           jsonStringB.contains("\"39955 49840\":{\"id\":[\"3\",\"4\"],\"name\":[\"c\",\"b\"],\"value\":[\"5.5\",\"4.0\"]}"))
  }

  test("test analyses no changes"){
    val dataA = Seq(
      (1, "a", 3.0, 11133),
      (4, "b", 4.0, 49840),
      (3, "c", 5.0, 39950)
    ).toDF("id", "name", "value", HASH_COLUMN_NAME)

    val dataB = Seq(
      (1, "a", 3.0, 11133),
      (4, "b", 4.0, 49840),
      (3, "c", 5.0, 39950)
    ).toDF("id", "name", "value", HASH_COLUMN_NAME)

    val diff = analyse(dataA, dataB)
    val jsonStringA = compact(render(parse(diff._1)))
    assert(jsonStringA.contains("{\"11133 11133\":{},\"49840 49840\":{},\"39950 39950\":{}}"))

    val jsonStringB = compact(render(parse(diff._2)))
    assert(jsonStringB.contains("{\"11133 11133\":{},\"49840 49840\":{},\"39950 39950\":{}}"))
  }



}


