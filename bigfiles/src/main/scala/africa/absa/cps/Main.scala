package africa.absa.cps

import africa.absa.cps.parser.ArgsParser
import africa.absa.cps.io.IOHandler
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.Paths

object Main {
  def main(args: Array[String]): Unit = {
    val arguments = ArgsParser.getArgs(args)

    implicit val spark: SparkSession = SparkSession.builder()
      .appName("DatasetComparator")
      .getOrCreate()

    import spark.implicits._

    // validate arguments
    ArgsParser.validate(arguments)

    // read data
    val dataA: DataFrame = IOHandler.sparkRead(arguments.inputA.toString)
    val dataB: DataFrame = IOHandler.sparkRead(arguments.inputB.toString)

    val (uniqA, uniqB) = Comparator.compare(dataA, dataB)

    // compute diff todo will be solved by issue #3

    val metrics: String = Comparator.createMetrics(dataA, dataB, uniqA, uniqB)

    // write to files
    val out = arguments.out.toString
    IOHandler.dfWrite(Paths.get(out, "inputA_differences").toString, uniqA)
    IOHandler.dfWrite(Paths.get(out, "inputB_differences").toString, uniqB)
    IOHandler.jsonWrite(Paths.get(out, "metrics.json").toString, metrics)

    // write diff todo will be solved by issue #3

  }
}

