import africa.absa.cps.parser.{ArgsParser, Arguments}
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

class ArgsParserTest extends AnyFunSuite{
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
    val args = Array[String]("-o", "src/test/resources/out", "--inputA", "src/test/resources/inputA.txt", "--inputB", "src/test/resources/inputB.txt")
    val res = ArgsParser.getArgs(args)
    assert(res.out.toString == "src/test/resources/out")
    assert(res.inputA.toString == "src/test/resources/inputA.txt")
    assert(res.inputB.toString == "src/test/resources/inputB.txt")
  }

  test("test that ArgParser will not throws exception if file does not exist") {
    val args = Array[String]("-o", "out", "--inputA", "file1", "--inputB", "file2")
    val res = ArgsParser.getArgs(args)
    assert(res.out.toString == "out")
    assert(res.inputA.toString == "file1")
    assert(res.inputB.toString == "file2")
  }

  ////////////////////////////////Validate//////////////////////////////////////////

  test("test that validate throws exception if inputA does not exist") {
    val wrongFile = new File("src/test/resources/file1")
    val args = Arguments(new File("src/test/resources/out"), wrongFile, new File("src/test/resources/inputB.txt"))
    val exception = intercept[IllegalArgumentException] {
      ArgsParser.validate(args)
    }
    assert(exception.getMessage == s"Input ${wrongFile.getAbsolutePath} does not exist")
  }

  test("test that validate throws exception if inputB does not exist") {
    val wrongFile = new File("src/test/resources/file2")
    val args = Arguments(new File("src/test/resources/out"), new File("src/test/resources/inputA.txt"), wrongFile)
    val exception = intercept[IllegalArgumentException] {
      ArgsParser.validate(args)
    }
    assert(exception.getMessage == s"Input ${wrongFile.getAbsolutePath} does not exist")
  }

  test("test that validate throws exception if output does not exist") {
    val wrongFile = new File("src/test/out")
    val args = Arguments(wrongFile, new File("src/test/resources/inputA.txt"), new File("src/test/resources/inputB.txt"))
    val exception = intercept[IllegalArgumentException] {
      ArgsParser.validate(args)
    }
    assert(exception.getMessage == s"Output ${wrongFile.getAbsolutePath} does not exist")
  }

  test("test that validate throws exception if output is a file") {
    val wrongFile = new File("src/test/resources/out.txt")
    val args = Arguments(wrongFile, new File("src/test/resources/inputA.txt"), new File("src/test/resources/inputB.txt"))
    val exception = intercept[IllegalArgumentException] {
      ArgsParser.validate(args)
    }
    assert(exception.getMessage == s"Output ${wrongFile.getAbsolutePath} is a file")
  }

  test("test that validate returns true if all files exist") {
    val args = Arguments(new File("src/test/resources/out"), new File("src/test/resources/inputA.txt"), new File("src/test/resources/inputB.txt"))
    assert(ArgsParser.validate(args))
  }
}
