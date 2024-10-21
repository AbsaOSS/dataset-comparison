package africa.absa.cps.parser

import java.io.File

case class Arguments(
                   out: File = new File("."),
                   inputA: File = new File("."),
                   inputB: File = new File("."),
                 )

