package africa.absa.cps.parser

case class Arguments(
                   out: String = "",
                   inputA: String = "",
                   inputB: String = "",
                   diff: DiffComputeType.Value = DiffComputeType.None,
                   exclude: Seq[String] = Seq()
                 )
