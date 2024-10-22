import africa.absa.cps.Comparator
import africa.absa.cps.hash.HashUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class HashTableTest extends AnyFunSuite{
  val spark: SparkSession = SparkTestSession.spark

  import spark.implicits._

  test("test that hash function always get same results for exactly the same input") {
    val df: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")
    val hash1 = HashUtils.createHashColumn(df)
    val hash2 = HashUtils.createHashColumn(df)
    assert(hash1.select(Comparator.HashName).head() == hash2.select(Comparator.HashName).head())
    assert(hash1.select(Comparator.HashName).head(2)(1) == hash2.select(Comparator.HashName).head(2)(1))
  }
  test("test that hash function always get same results for  same input") {
    val df1: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")
    val df2: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")
    val hash1 = HashUtils.createHashColumn(df1)
    val hash2 = HashUtils.createHashColumn(df2)
    assert(hash1.select(Comparator.HashName).head() == hash2.select(Comparator.HashName).head())
    assert(hash1.select(Comparator.HashName).head(2)(1) == hash2.select(Comparator.HashName).head(2)(1))
  }
  test("test that hash function always get different results for different input") {
    val df1: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")
    val df2: DataFrame = Seq((3, "three"), (4, "four")).toDF("id", "value")
    val hash1 = HashUtils.createHashColumn(df1)
    val hash2 = HashUtils.createHashColumn(df2)
    assert(hash1.select(Comparator.HashName).head() != hash2.select(Comparator.HashName).head())
    assert(hash1.select(Comparator.HashName).head(2)(1) != hash2.select(Comparator.HashName).head(2)(1))
  }
}
