package africa.absa.cps.parser

import scopt.OParser

import java.io.File

object ArgsParser {
  def getArgs(args: Array[String]): Option[(String, String, String)] = {
    val builder = OParser.builder[Config]
    val parser1 = {
      import builder._
      OParser.sequence(
        programName("Dataset Comparison"),
        head("dataset-comparison", "0.x"),
        opt[File]('o', "out") // output filepath
          .required()
          .valueName("<file>")
          .action((x, c) => c.copy(out = x))
          .text("output path"),
        arg[File]("<file>...") // two files for comparison
          .required()
          .minOccurs(2)
          .maxOccurs(2)
          .action((x, c) => c.copy(files = c.files :+ x))
          .text("2 input files paths to compare"),
      )
    }

    // match the arguments with the parser
    OParser.parse(parser1, args, Config()) match {
      case Some(config) => Some((config.files.head.toString, config.files(1).toString, config.out.toString))
      case _ => None
    }
  }
}
