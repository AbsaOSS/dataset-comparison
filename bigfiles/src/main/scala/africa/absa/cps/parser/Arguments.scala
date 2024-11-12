package africa.absa.cps.parser


case class Arguments(
                   out: String = "",
                   inputA: String = "",
                   inputB: String = "",
                   fsURI: String = "",
                   exclude: Seq[String] = Seq()
                 )

