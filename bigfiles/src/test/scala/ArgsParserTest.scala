import africa.absa.cps.parser.ArgsParser
import org.scalatest.funsuite.AnyFunSuite

class ArgsParserTest extends AnyFunSuite{
  test("test that ArgParser returns None if no arguments are passed") {
    val args = Array[String]()
    assert(ArgsParser.getArgs(args).isEmpty)
  }
  test("test that ArgParser returns None if only one file is passed") {
    val args = Array[String]("-o", "out", "file1")
    assert(ArgsParser.getArgs(args).isEmpty)
  }
  test("test that ArgParser returns None if more than two files are passed") {
    val args = Array[String]("-o", "out", "file1", "file2", "file3")
    assert(ArgsParser.getArgs(args).isEmpty)
  }
  test("test that ArgParser returns arguments if correct arguments are passed") {
    val args = Array[String]("-o", "out", "file1", "file2")
    println(ArgsParser.getArgs(args).get)
    assert(ArgsParser.getArgs(args).get == ("file1", "file2", "out"))
  }
}
