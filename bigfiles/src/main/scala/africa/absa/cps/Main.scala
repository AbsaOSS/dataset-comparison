package africa.absa.cps

import africa.absa.cps.parser.ArgsParser
import africa.absa.cps.io.IOHandler
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {
  val HashName = "cps_comparison_hash"
  def main(args: Array[String]): Unit = {
    val arguments = ArgsParser.getArgs(args)

    implicit val spark: SparkSession = SparkSession.builder()
      .appName("DatasetComparator")
      .getOrCreate()

    import spark.implicits._

    // read data
    val dataA: DataFrame = IOHandler.sparkRead(arguments.inputA.toString)
    val dataB: DataFrame = IOHandler.sparkRead(arguments.inputB.toString)

    val (uniqA, uniqB) = Comparator.compare(dataA, dataB, arguments.out.toString)

    // compute diff todo will be solved by issue #3

    val metrics: String = Comparator.createMetrics(dataA, dataB, uniqA, uniqB)

    // write to files
    IOHandler.dfWrite(arguments.out + "/inputA_differences", uniqA)
    IOHandler.dfWrite(arguments.out + "/inputB_differences", uniqB)
    IOHandler.jsonWrite(arguments.out + "metrics.json", metrics)

    // write diff todo will be solved by issue #3

    // show different rows
    println("Different rows: ")
    uniqA.show(numRows = 5, truncate = false)
    uniqB.show(numRows = 5, truncate = false)
  }
}

