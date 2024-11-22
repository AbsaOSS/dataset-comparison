package africa.absa.cps.parser

case class Arguments(
    out: String = "",
    inputA: String = "",
    inputB: String = "",
    exclude: Seq[String] = Seq()
)
