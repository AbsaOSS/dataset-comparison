package africa.absa.cps.parser

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import scopt.OParser

import java.net.URI

object ArgsParser {

  /** Read the arguments from the command line by using the scopt library
    * @param args
    *   arguments from the command line
    * @return
    *   Config class containing the arguments
    */
  def getArgs(args: Array[String]): Arguments = {
    val builder = OParser.builder[Arguments]
    val parser1 = {
      import builder._
      OParser.sequence(
        programName("Dataset Comparison"),
        head("dataset-comparison", "0.x"),
        opt[String]('o', "out") // output filepath
          .required()
          .valueName("<file>")
          .action((x, c) => c.copy(out = x))
          .text("output path to directory"),
        opt[String]('f', "format") // output format
          .optional()
          .action((x, c) => c.copy(outFormat = OutputFormatType.withName(x)))
          .text("output format. You can chose from: (parquet, csv)"),
        opt[String]("inputA") // path to first input
          .required()
          .valueName("<file>")
          .action((x, c) => c.copy(inputA = x))
          .text("inputA paths to compare"),
        opt[String]("inputB") // path to second input
          .required()
          .valueName("<file>")
          .action((x, c) => c.copy(inputB = x))
          .text("inputB paths to compare"),
        opt[String]('d', "diff") // diff type
          .optional()
          .action((x, c) => c.copy(diff = DiffComputeType.withName(x)))
          .text("Compute differences. You can chose from: (Row)"),
        opt[Seq[String]]('e', "exclude") // columns to exclude
          .valueName("<column1>,<column2>...")
          .action((x, c) => c.copy(exclude = x))
          .text(
            "columns to exclude. Default: empty. Columns will be" +
              " excluded from both tables if they are present." +
              " Dont put spaces between columns only commas."
          )
      )
    }

    // match the arguments with the parser
    OParser.parse(parser1, args, Arguments()) match {
      case Some(config) => config
      case _            => throw new IllegalArgumentException("Invalid arguments")
    }
  }

  /** Validate the arguments
    * @param args
    *   arguments to validate
    * @return
    *   true if the arguments are valid
    */
  def validate(args: Arguments)(implicit spark: SparkSession): Boolean = {
    val config = spark.sparkContext.hadoopConfiguration
    val fs     = FileSystem.get(config)
    if (!fs.exists(new Path(args.inputA))) throw new IllegalArgumentException(s"Input ${args.inputA} does not exist")
    if (!fs.exists(new Path(args.inputB))) throw new IllegalArgumentException(s"Input ${args.inputB} does not exist")
    if (fs.exists(new Path(args.out))) throw new IllegalArgumentException(s"Output ${args.out} already exist")
    true
  }
}
