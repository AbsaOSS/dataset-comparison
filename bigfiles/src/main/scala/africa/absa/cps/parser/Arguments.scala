package africa.absa.cps.parser


object DiffComputeType extends Enumeration {
  val None, Row = Value
}
object DiffComputeTypeHelper {
  implicit val diffComputeRead: scopt.Read[DiffComputeType.Value] = scopt.Read.reads(DiffComputeType.withName)
}

case class Arguments(
                   out: String = "",
                   inputA: String = "",
                   inputB: String = "",
                   diff: DiffComputeType.Value = DiffComputeType.None,
                   exclude: Seq[String] = Seq()
                 )

