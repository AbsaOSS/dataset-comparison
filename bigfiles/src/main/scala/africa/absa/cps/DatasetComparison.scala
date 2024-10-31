package africa.absa.cps

import africa.absa.cps.parser.ArgsParser
import africa.absa.cps.io.IOHandler
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.Paths

object DatasetComparison {
  def main(args: Array[String]): Unit = {
    val arguments = ArgsParser.getArgs(args)

    // validate arguments
    ArgsParser.validate(arguments)

    implicit val spark: SparkSession = SparkSession.builder()
      .appName("DatasetComparator")
      .config("spark.hadoop.fs.default.name", arguments.fsURI)
      .config("spark.hadoop.fs.defaultFS", arguments.fsURI)
      .config("spark.hadoop.fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
      .config("spark.hadoop.fs.hdfs.server", classOf[org.apache.hadoop.hdfs.server.namenode.NameNode].getName)
      .config("spark.hadoop.conf", classOf[org.apache.hadoop.hdfs.HdfsConfiguration].getName)
      .getOrCreate()

    import spark.implicits._

    // read data
    val dataA: DataFrame = IOHandler.sparkRead(arguments.inputA)
    val dataB: DataFrame = IOHandler.sparkRead(arguments.inputB)

    val (uniqA, uniqB) = Comparator.compare(dataA, dataB)

    // compute diff todo will be solved by issue #3

    val metrics: String = Comparator.createMetrics(dataA, dataB, uniqA, uniqB)

    // write to files
    val out = arguments.out
    IOHandler.dfWrite(Paths.get(out, "inputA_differences").toString, uniqA)
    IOHandler.dfWrite(Paths.get(out, "inputB_differences").toString, uniqB)
    IOHandler.jsonWrite(Paths.get(out, "metrics.json").toString, metrics)

    // write diff todo will be solved by issue #3

  }
}

