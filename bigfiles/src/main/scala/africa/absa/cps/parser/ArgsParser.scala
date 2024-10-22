package africa.absa.cps.parser

import scopt.OParser

import java.io.File

object ArgsParser {
  /**
   * Read the arguments from the command line by using the scopt library
   * @param args arguments from the command line
   * @return Config class containing the arguments
   */
  def getArgs(args: Array[String]): Arguments = {
    val builder = OParser.builder[Arguments]
    val parser1 = {
      import builder._
      OParser.sequence(
        programName("Dataset Comparison"),
        head("dataset-comparison", "0.x"),
        opt[File]('o', "out") // output filepath
          .required()
          .valueName("<file>")
          .action((x, c) => c.copy(out = x))
          .text("output path to directory"),
        opt[File]("inputA") // path to first input
          .required()
          .valueName("<file>")
          .action((x, c) => c.copy(inputA = x))
          .text("inputA paths to compare"),
        opt[File]("inputB") // path to second input
          .required()
          .valueName("<file>")
          .action((x, c) => c.copy(inputB = x))
          .text("inputB paths to compare")
      )
    }

    // match the arguments with the parser
    OParser.parse(parser1, args, Arguments()) match {
      case Some(config) => config
      case _ => throw new IllegalArgumentException("Invalid arguments")
    }
  }

  /**
   * Validate the arguments
   * @param args arguments to validate
   * @return true if the arguments are valid
   */
  def validate(args: Arguments): Boolean = {
    if (!args.inputA.exists()) throw new IllegalArgumentException(s"Input ${args.inputA.getAbsolutePath} does not exist")
    if (!args.inputB.exists()) throw new IllegalArgumentException(s"Input ${args.inputB.getAbsolutePath} does not exist")
    if (!args.out.exists()) throw new IllegalArgumentException(s"Output ${args.out.getAbsolutePath} does not exist")
    if (!args.out.isDirectory) throw new IllegalArgumentException(s"Output ${args.out.getAbsolutePath} is a file")
    true
  }
}
