package africa.absa.cps.parser

object OutputFormatType extends Enumeration {
  type OutputFormatType = Value
  val Parquet: OutputFormatType = Value("parquet")
  val CSV: OutputFormatType     = Value("csv")

  override def toString: String = super.toString
}
