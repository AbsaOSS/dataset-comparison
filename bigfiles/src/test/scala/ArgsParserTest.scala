import africa.absa.cps.parser.{ArgsParser, Arguments}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.net.URI

class ArgsParserTest extends AnyFunSuite with BeforeAndAfterAll{
  val FS_URI: String =  new File("src/test/resources").getAbsoluteFile.toURI.toString
  val FS: FileSystem = FileSystem.get(new URI(FS_URI), new Configuration())

  override def beforeAll(): Unit = {
    FS.setWorkingDirectory(new Path(FS_URI))
  }

  test("test that ArgParser throws exception if no arguments are passed") {
    val args = Array[String]()
    assertThrows[IllegalArgumentException](ArgsParser.getArgs(args))
  }

  test("test that ArgParser throws exception if only one file is passed") {
    val args = Array[String]("-o", "out", "--inputA", "file1", "--fsURI", FS_URI)
    assertThrows[IllegalArgumentException](ArgsParser.getArgs(args))
  }

  test("test that ArgParser throws exception if one more file is passed") {
    val args = Array[String]("-o", "out", "--inputA", "file1", "--inputB", "file2", "file3", "--fsURI", FS_URI)
    assertThrows[IllegalArgumentException](ArgsParser.getArgs(args))
  }


  test("test that we can not pass more then one output, it will throw exception") {
    val args = Array[String]("-o", "out", "-o", "out2",  "--inputA", "file1", "--inputB", "file2", "--fsURI", FS_URI)
    assertThrows[IllegalArgumentException](ArgsParser.getArgs(args))
  }

  test("test that we can not pass more then one inputA, it will throw exception") {
    val args = Array[String]("-o", "out", "--inputA", "file1", "--inputA", "fileA", "--inputB", "file2", "--fsURI", FS_URI)
    assertThrows[IllegalArgumentException](ArgsParser.getArgs(args))
  }

  test("test that we can not pass more then one inputB, it will throw exception") {
    val args = Array[String]("-o", "out", "--inputA", "file1", "--inputB", "fileB", "--inputB", "file2", "--fsURI", FS_URI)
    assertThrows[IllegalArgumentException](ArgsParser.getArgs(args))
  }

  test("test that ArgParser returns arguments if correct arguments are passed") {
    val args = Array[String]("-o", "out", "--inputA", "inputA.txt", "--inputB", "inputB.txt", "--fsURI", FS_URI)
    val res = ArgsParser.getArgs(args)
    assert(res.out == "out")
    assert(res.inputA == "inputA.txt")
    assert(res.inputB == "inputB.txt")
  }

  test("test that ArgParser will not throws exception if file does not exist") {
    val args = Array[String]("-o", "out", "--inputA", "file1", "--inputB", "file2", "--fsURI", FS_URI)
    val res = ArgsParser.getArgs(args)
    assert(res.out == "out")
    assert(res.inputA == "file1")
    assert(res.inputB == "file2")
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
  test("test that validate throws exception if inputA does not exist") {
    val wrongFile = "file1"
    val args = Arguments("out", wrongFile, "inputB.txt", FS_URI)
    val exception = intercept[IllegalArgumentException] {
      ArgsParser.validate(args)
    }
    assert(exception.getMessage == s"Input ${wrongFile} does not exist")
  }

  test("test that validate throws exception if inputB does not exist") {
    val wrongFile = "file2"
    val args = Arguments("out", "inputA.txt", wrongFile, FS_URI)
    val exception = intercept[IllegalArgumentException] {
      ArgsParser.validate(args)
    }
    assert(exception.getMessage == s"Input ${wrongFile} does not exist")
  }

  test("test that validate throws exception if output exists") {
    val wrongFile = "out"
    val args = Arguments(wrongFile, "inputA.txt", "inputB.txt", FS_URI)
    val exception = intercept[IllegalArgumentException] {
      ArgsParser.validate(args)
    }
    assert(exception.getMessage == s"Output ${wrongFile} already exist")
  }

  test("test that validate returns true if all files exist") {
    val args = Arguments("output", "inputA.txt", "inputB.txt", FS_URI)
    assert(ArgsParser.validate(args))
  }
}
