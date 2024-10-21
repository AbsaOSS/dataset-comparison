package africa.absa.cps

import africa.absa.cps.parser.ArgsParser
import africa.absa.cps.readers.SparkReader
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {
  val HashName = "CPS_hash"
  def main(args: Array[String]): Unit = {
    val arguments = ArgsParser.getArgs(args)

    val spark = SparkSession.builder()
      .appName("DatasetComparator")
      .master("local[*]") //todo remove
      .getOrCreate()

    import spark.implicits._

    val dataA: DataFrame = SparkReader.read(arguments.inputA.toString, spark)
    val dataB: DataFrame = SparkReader.read(arguments.inputB.toString, spark)
    Comparator.compare(dataA, dataB, arguments.out.toString, spark)
  }
}

