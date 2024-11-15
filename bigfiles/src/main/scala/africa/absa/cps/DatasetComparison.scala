package africa.absa.cps

import africa.absa.cps.analysis.RowByRowAnalysis
import africa.absa.cps.parser.{ArgsParser, DiffComputeType}
import africa.absa.cps.io.IOHandler
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.native.JsonMethods.{compact, parse, render}
import com.typesafe.config.ConfigFactory
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.Paths

object DatasetComparison {
  private val logger: Logger = LoggerFactory.getLogger(DatasetComparison.getClass)

  def main(args: Array[String]): Unit = {
    val arguments = ArgsParser.getArgs(args)

    implicit val spark: SparkSession = SparkSession.builder()
      .appName("DatasetComparator")
      .getOrCreate()

    import spark.implicits._

    // validate arguments
    ArgsParser.validate(arguments)

    // read data
    val dataA: DataFrame = DatasetComparisonHelper.exclude(IOHandler.sparkRead(arguments.inputA), arguments.exclude, "A")
    val dataB: DataFrame = DatasetComparisonHelper.exclude(IOHandler.sparkRead(arguments.inputB), arguments.exclude, "B")

    val (uniqA, uniqB) = Comparator.compare(dataA, dataB)


    val metrics: String = Comparator.createMetrics(dataA, dataB, uniqA, uniqB, arguments.exclude)

    // write to files
    val out = arguments.out
    IOHandler.dfWrite(Paths.get(out, "inputA_differences").toString, uniqA)
    IOHandler.dfWrite(Paths.get(out, "inputB_differences").toString, uniqB)
    IOHandler.jsonWrite(Paths.get(out, "metrics.json").toString, metrics)

    // read config
    val conf = ConfigFactory.load()
    val threshold = conf.getInt("dataset-comparison.analysis.diff-threshold")

    if (arguments.diff == DiffComputeType.Row && uniqA.count() <= threshold && uniqB.count() <= threshold) {
      // compute diff
      val diff = RowByRowAnalysis.analyse(uniqA, uniqB)

      // write diff
      IOHandler.jsonWrite(Paths.get(out, "A_to_B_changes.json").toString, compact(render(parse(diff._1))))
      IOHandler.jsonWrite(Paths.get(out, "B_to_A_changes.json").toString, compact(render(parse(diff._1))))
    }
    else if (arguments.diff == DiffComputeType.Row){
      logger.warn("The number of differences is too large to compute row by row differences.")
    }
  }
}
