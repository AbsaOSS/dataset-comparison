package africa.absa.cps.parser

import java.io.File

case class Config(
                   files: Seq[File] = Seq(),
                   out: File = new File("."),
                 )

