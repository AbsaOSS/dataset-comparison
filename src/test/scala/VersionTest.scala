package africa.absa.cps

import org.apache.spark._
import org.scalatest.funsuite.AnyFunSuite

class VersionTest extends AnyFunSuite {
  test("test spark version") {
    val sc = new SparkContext("local[*]", "AppName")
    assert(sc.version === "3.5.3")
  }
}
