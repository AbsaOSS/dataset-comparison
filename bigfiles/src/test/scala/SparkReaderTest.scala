import africa.absa.cps.readers.SparkReader
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.reflect.io.Directory

class SparkReaderTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  val spark: SparkSession = SparkTestSession.spark

  import spark.implicits._

  val filePath = "src/test/resources/sample.parquet"

  override def afterAll(): Unit = {
    // Clean up
    new Directory(new File(filePath)).deleteRecursively()
  }

  test("test that SparkReader reads a Parquet file into a DataFrame") {
    val tmp: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")
    tmp.write.mode(SaveMode.Overwrite).format("parquet").save(filePath)

    val df: DataFrame = SparkReader.read(filePath, spark)

    assert(df != null)
    assert(df.count() > 0)
  }
}

