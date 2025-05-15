package africa.absa.cps.parser


case class Arguments(
                      out: String = "",
                      inputA: String = "",
                      inputB: String = "",
                      outFormat: OutputFormatType.Value = OutputFormatType.Parquet,
                      diff: DiffComputeType.Value = DiffComputeType.None,
                      exclude: Seq[String] = Seq()
)
