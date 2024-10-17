import africa.absa.cps.hash.HashTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class HashTableTest  extends AnyFunSuite{
  val spark: SparkSession = SparkTestSession.spark

  import spark.implicits._

  test("test that hash function always get same results for same input") {
    val tmp: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")
    val hash1 = HashTable.hash(tmp)
    val hash2 = HashTable.hash(tmp)
    assert(hash1.select("hash").head() == hash2.select("hash").head())
    assert(hash1.select("hash").head(2)(1) == hash2.select("hash").head(2)(1))
  }
  test("test that hash function always get different results for different input") {
    val tmp1: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")
    val tmp2: DataFrame = Seq((3, "three"), (4, "four")).toDF("id", "value")
    val hash1 = HashTable.hash(tmp1)
    val hash2 = HashTable.hash(tmp2)
    assert(hash1.select("hash").head() != hash2.select("hash").head())
    assert(hash1.select("hash").head(2)(1) != hash2.select("hash").head(2)(1))
  }
}
