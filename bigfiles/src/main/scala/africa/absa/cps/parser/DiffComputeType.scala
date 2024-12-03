package africa.absa.cps.parser

object DiffComputeType extends Enumeration {
  val None, Row = Value
}
object DiffComputeTypeHelper {
  implicit val diffComputeRead: scopt.Read[DiffComputeType.Value] = scopt.Read.reads(DiffComputeType.withName)
}
