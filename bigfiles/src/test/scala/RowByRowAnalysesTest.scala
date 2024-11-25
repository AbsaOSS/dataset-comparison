
import africa.absa.cps.analysis.{ColumnsDiff, RowsDiff}
import africa.absa.cps.analysis.RowByRowAnalysis.analyse
import africa.absa.cps.hash.HashUtils.HASH_COLUMN_NAME
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.json4s.native.JsonMethods.{compact, parse, render}
import org.scalatest.funsuite.AnyFunSuite
import upickle.default._


class RowByRowAnalysesTest extends AnyFunSuite{
  implicit val spark: SparkSession = SparkTestSession.spark
  import spark.implicits._
  implicit val ColumnsDiffRw: ReadWriter[ColumnsDiff] = macroRW
  implicit val RowDiffRw: ReadWriter[RowsDiff] = macroRW
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
    val rowsDiffListA: List[RowsDiff] = read[List[RowsDiff]](diff._1)

    assert(rowsDiffListA.length == 3)
    assert(rowsDiffListA.contains(RowsDiff(inputAHash = "11133", inputBHash = "19830", diffs = List(ColumnsDiff(columnName = "name", values = List("a", "b"))))))
    assert(rowsDiffListA.contains(RowsDiff(inputAHash = "49840", inputBHash = "29845", diffs = List(
        ColumnsDiff(columnName = "id", values = List("4", "2")),
        ColumnsDiff(columnName = "value", values = List("4.0", "4.5")))))
      ||
      rowsDiffListA.contains(RowsDiff(inputAHash = "49840", inputBHash = "19830", diffs = List(
        ColumnsDiff(columnName = "id", values = List("4", "1")),
        ColumnsDiff(columnName = "value", values = List("4.0", "3.0")))))
    )
    assert(rowsDiffListA.contains(RowsDiff(inputAHash = "39950", inputBHash = "39955", diffs = List(ColumnsDiff(columnName = "value", values = List("5.0", "3.0"))))))


    val rowsDiffListB: List[RowsDiff] = read[List[RowsDiff]](diff._2)

    assert(rowsDiffListB.length == 3)
    assert(rowsDiffListB.contains(RowsDiff(inputAHash = "19830", inputBHash = "11133", diffs = List(ColumnsDiff(columnName = "name", values = List("b", "a"))))))
    assert(rowsDiffListB.contains(RowsDiff(inputAHash = "29845", inputBHash = "49840", diffs = List(
      ColumnsDiff(columnName = "id", values = List("2", "4")),
      ColumnsDiff(columnName = "value", values = List("4.5", "4.0")))))
    )
    assert(rowsDiffListB.contains(RowsDiff(inputAHash = "39955", inputBHash = "39950", diffs = List(ColumnsDiff(columnName = "value", values = List("3.0", "5.0"))))))


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
    val rowsDiffListA: List[RowsDiff] = read[List[RowsDiff]](diff._1)

    assert(rowsDiffListA.length == 3)
    assert(rowsDiffListA.contains(RowsDiff(inputAHash = "11133", inputBHash = "19830", diffs = List(ColumnsDiff(columnName = "value", values = List("3.0", "3.5"))))))
    assert(rowsDiffListA.contains(RowsDiff(inputAHash = "49840", inputBHash = "29845", diffs = List(ColumnsDiff(columnName = "value", values = List("4.0", "4.5"))))))
    assert(rowsDiffListA.contains(RowsDiff(inputAHash = "39950", inputBHash = "39955", diffs = List(ColumnsDiff(columnName = "value", values = List("5.0", "5.5"))))))

    val rowsDiffListB: List[RowsDiff] = read[List[RowsDiff]](diff._2)

    assert(rowsDiffListB.length == 3)
    assert(rowsDiffListB.contains(RowsDiff(inputAHash = "19830", inputBHash = "11133", diffs = List(ColumnsDiff(columnName = "value", values = List("3.5", "3.0"))))))
    assert(rowsDiffListB.contains(RowsDiff(inputAHash = "29845", inputBHash = "49840", diffs = List(ColumnsDiff(columnName = "value", values = List("4.5", "4.0"))))))
    assert(rowsDiffListB.contains(RowsDiff(inputAHash = "39955", inputBHash = "39950", diffs = List(ColumnsDiff(columnName = "value", values = List("5.5", "5.0"))))))
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

    val rowsDiffListA: List[RowsDiff] = read[List[RowsDiff]](diff._1)

    assert(rowsDiffListA.length == 3)
    assert(rowsDiffListA.contains(RowsDiff(inputAHash = "11133", inputBHash = "19830", diffs = List(ColumnsDiff(columnName = "name", values = List("a", "b"))))))
    assert(rowsDiffListA.contains(RowsDiff(inputAHash = "49840", inputBHash = "29845", diffs = List(ColumnsDiff(columnName = "value", values = List("4.0", "4.5"))))))
    assert(rowsDiffListA.contains(RowsDiff(inputAHash = "39950", inputBHash = "39955", diffs = List(ColumnsDiff(columnName = "id", values = List("3", "4"))))))

    val rowsDiffListB: List[RowsDiff] = read[List[RowsDiff]](diff._2)

    assert(rowsDiffListB.length == 3)
    assert(rowsDiffListB.contains(RowsDiff(inputAHash = "19830", inputBHash = "11133", diffs = List(ColumnsDiff(columnName = "name", values = List("b", "a"))))))
    assert(rowsDiffListB.contains(RowsDiff(inputAHash = "29845", inputBHash = "49840", diffs = List(ColumnsDiff(columnName = "value", values = List("4.5", "4.0"))))))
    assert(rowsDiffListB.contains(RowsDiff(inputAHash = "39955", inputBHash = "39950", diffs = List(ColumnsDiff(columnName = "id", values = List("4", "3"))))))
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

    val rowsDiffListA: List[RowsDiff] = read[List[RowsDiff]](diff._1)

    assert(rowsDiffListA.length == 2)
    assert(rowsDiffListA.contains(RowsDiff(inputAHash = "11133", inputBHash = "19830", diffs = List(ColumnsDiff(columnName = "value", values = List("3.0", "3.5"))))))
    assert(rowsDiffListA.contains(RowsDiff(inputAHash = "49840", inputBHash = "29845", diffs = List(ColumnsDiff(columnName = "value", values = List("4.0", "4.5"))))))

    val rowsDiffListB: List[RowsDiff] = read[List[RowsDiff]](diff._2)

    assert(rowsDiffListB.length == 3)
    assert(rowsDiffListB.contains(RowsDiff(inputAHash = "19830", inputBHash = "11133", diffs = List(ColumnsDiff(columnName = "value", values = List("3.5", "3.0"))))))
    assert(rowsDiffListB.contains(RowsDiff(inputAHash = "29845", inputBHash = "49840", diffs = List(ColumnsDiff(columnName = "value", values = List("4.5", "4.0"))))))
    assert(rowsDiffListB.contains(RowsDiff(inputAHash = "39955", inputBHash = "11133", diffs = List(
      ColumnsDiff(columnName = "id", values = List("3", "1")),
      ColumnsDiff(columnName = "name", values = List("c", "a")),
      ColumnsDiff(columnName = "value", values = List("5.5", "3.0"))))) ||
      rowsDiffListB.contains(RowsDiff(inputAHash = "39955", inputBHash = "49840", diffs = List(
        ColumnsDiff(columnName = "id", values = List("3", "4")),
        ColumnsDiff(columnName = "name", values = List("c", "b")),
        ColumnsDiff(columnName = "value", values = List("5.5", "4.0")))))
    )

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
    val rowsDiffListA: List[RowsDiff] = read[List[RowsDiff]](diff._1)

    assert(rowsDiffListA.length == 3)
    assert(rowsDiffListA.contains(RowsDiff(inputAHash = "11133", inputBHash = "11133", diffs = List())))
    assert(rowsDiffListA.contains(RowsDiff(inputAHash = "49840", inputBHash = "49840", diffs = List())))
    assert(rowsDiffListA.contains(RowsDiff(inputAHash = "39950", inputBHash = "39950", diffs = List())))

    val rowsDiffListB: List[RowsDiff] = read[List[RowsDiff]](diff._2)

    assert(rowsDiffListB.length == 3)
    assert(rowsDiffListB.contains(RowsDiff(inputAHash = "11133", inputBHash = "11133", diffs = List())))
    assert(rowsDiffListB.contains(RowsDiff(inputAHash = "49840", inputBHash = "49840", diffs = List())))
    assert(rowsDiffListB.contains(RowsDiff(inputAHash = "39950", inputBHash = "39950", diffs = List())))
  }



}


