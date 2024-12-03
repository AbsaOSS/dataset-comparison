import africa.absa.cps.parser.{ArgsParser, Arguments, DiffComputeType}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.net.URI

class ArgsParserTest extends AnyFunSuite with BeforeAndAfterAll{
  val FS_URI: String = SparkTestSession.FS_URI
  val FS: FileSystem = FileSystem.get(new URI(FS_URI), new Configuration())

  override def beforeAll(): Unit = {
    FS.setWorkingDirectory(new Path(FS_URI))
  }

  test("test that ArgParser throws exception if no arguments are passed") {
    val args = Array[String]()
    assertThrows[IllegalArgumentException](ArgsParser.getArgs(args))
  }

  test("test that ArgParser throws exception if only one file is passed") {
    val args = Array[String]("-o", "out", "--inputA", "file1")
    assertThrows[IllegalArgumentException](ArgsParser.getArgs(args))
  }

  test("test that ArgParser throws exception if one more file is passed") {
    val args = Array[String]("-o", "out", "--inputA", "file1", "--inputB", "file2", "file3")
    assertThrows[IllegalArgumentException](ArgsParser.getArgs(args))
  }


  test("test that we can not pass more then one output, it will throw exception") {
    val args = Array[String]("-o", "out", "-o", "out2",  "--inputA", "file1", "--inputB", "file2")
    assertThrows[IllegalArgumentException](ArgsParser.getArgs(args))
  }

  test("test that we can not pass more then one inputA, it will throw exception") {
    val args = Array[String]("-o", "out", "--inputA", "file1", "--inputA", "fileA", "--inputB", "file2")
    assertThrows[IllegalArgumentException](ArgsParser.getArgs(args))
  }

  test("test that we can not pass more then one inputB, it will throw exception") {
    val args = Array[String]("-o", "out", "--inputA", "file1", "--inputB", "fileB", "--inputB", "file2")
    assertThrows[IllegalArgumentException](ArgsParser.getArgs(args))
  }

  test("test that ArgParser returns arguments if correct arguments are passed") {
    val args = Array[String]("-o", "out", "--inputA", "inputA.txt", "--inputB", "inputB.txt")
    val res = ArgsParser.getArgs(args)
    assert(res.out == "out")
    assert(res.inputA == "inputA.txt")
    assert(res.inputB == "inputB.txt")
  }

  test("test that ArgParser will not throws exception if file does not exist") {
    val args = Array[String]("-o", "out", "--inputA", "file1", "--inputB", "file2")
    val res = ArgsParser.getArgs(args)
    assert(res.out == "out")
    assert(res.inputA == "file1")
    assert(res.inputB == "file2")
  }

  test("test diff option will be set to Row if passed") {
    val args = Array[String]("-o", "out", "--inputA", "file1", "--inputB", "file2", "--diff", "Row")
    val res = ArgsParser.getArgs(args)
    assert(res.diff == DiffComputeType.Row)

    val args2 = Array[String]("-o", "out", "--inputA", "file1", "--inputB", "file2", "-d", "Row")
    val res2 = ArgsParser.getArgs(args2)
    assert(res2.diff == DiffComputeType.Row)
  }
  test("test diff option will be set to None if diff option is not passed") {
    val args = Array[String]("-o", "out", "--inputA", "file1", "--inputB", "file2")
    val res = ArgsParser.getArgs(args)
    assert(res.diff == DiffComputeType.None)
  }

  test("test that ArgParser will throw exception if diff option is not valid") {
    val args = Array[String]("-o", "out", "--inputA", "file1", "--inputB", "file2", "-d", "NotValid")
    assertThrows[IllegalArgumentException](ArgsParser.getArgs(args))
  }


  test("test that exclude option is correctly parsed") {
    val args = Array[String]("-o", "out", "--inputA", "file1", "--inputB", "file2", "--exclude", "col1")
    val res = ArgsParser.getArgs(args)
    assert(res.exclude == Seq("col1"))
  }

  test("test that exclude option is correctly parsed with multiple columns") {
    val args = Array[String]("-o", "out", "--inputA", "file1", "--inputB", "file2", "--exclude", "col1,col2,col3")
    val res = ArgsParser.getArgs(args)
    assert(res.exclude == Seq("col1", "col2", "col3"))
  }

  test("test that ArgParses has empty exclude if exclude is not passed") {
    val args = Array[String]("-o", "out", "--inputA", "file1", "--inputB", "file2")
    val res = ArgsParser.getArgs(args)
    assert(res.exclude == Seq())
  }

  ////////////////////////////////Validate//////////////////////////////////////////
//  val HDFS_URI = "hdfs://localhost:9999/"
//  override def beforeAll(): Unit = {
//    val hdfs = FileSystem.get(new URI(HDFS_URI), new Configuration())
//    val dirPath = "testDir"
//    hdfs.mkdirs(new Path(dirPath))
//    hdfs.copyFromLocalFile(new Path("src/test/resources/out"), new Path(dirPath + "/out"))
//    hdfs.copyFromLocalFile(new Path("src/test/resources/inputA.txt"), new Path(dirPath + "/inputA.txt"))
//    hdfs.copyFromLocalFile(new Path("src/test/resources/inputB.txt"), new Path(dirPath + "/inputB.txt"))
//  }
//
//  test("test hadoop ...") {
//    val file = "/foo/test.csv"
//    val args = Arguments("/foo/out", file, file, HDFS_URI)
//    assert(ArgsParser.validate(args))
//  }

  implicit val spark: SparkSession = SparkTestSession.spark
  import spark.implicits._


  test("test that validate throws exception if inputA does not exist") {
    val wrongFile = "file1"
    val args = Arguments("out", wrongFile, "inputB.txt")
    val exception = intercept[IllegalArgumentException] {
      ArgsParser.validate(args)
    }
    assert(exception.getMessage == s"Input ${wrongFile} does not exist")
  }

  test("test that validate throws exception if inputB does not exist") {
    val wrongFile = "file2"
    val args = Arguments("out", "inputA.txt", wrongFile)
    val exception = intercept[IllegalArgumentException] {
      ArgsParser.validate(args)
    }
    assert(exception.getMessage == s"Input ${wrongFile} does not exist")
  }

  test("test that validate throws exception if output exists") {
    val wrongFile = "out"
    val args = Arguments(wrongFile, "inputA.txt", "inputB.txt")
    val exception = intercept[IllegalArgumentException] {
      ArgsParser.validate(args)
    }
    assert(exception.getMessage == s"Output ${wrongFile} already exist")
  }

  test("test that validate returns true if all files exist") {
    val args = Arguments("output", "inputA.txt", "inputB.txt")
    assert(ArgsParser.validate(args))
  }
}
