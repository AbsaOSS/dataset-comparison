import africa.absa.cps.Comparator
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import scala.reflect.io.Directory

class ComparatorTest extends AnyFunSuite with BeforeAndAfterAll {
  val spark: SparkSession = SparkTestSession.spark

  import spark.implicits._

  val filePath = "src/test/resources/sample.parquet"
  val outputPath = "src/test/resources/output"

  override def afterAll(): Unit = {
    // Clean up
    new Directory(new File(filePath)).deleteRecursively()
    new Directory(new File(outputPath)).deleteRecursively()
  }

  test("test that all files are created"){
    val tmp: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")
    tmp.write.mode(SaveMode.Overwrite).format("parquet").save(filePath)

    Comparator.compare(filePath, filePath, outputPath, spark)

    val files = new File(outputPath).listFiles().map(_.getName)
    assert(files.length == 3)

  }
}
