package africa.absa.cps.parser

import java.io.File

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
                   fsURI: String = "",
                   diff: DiffComputeType.Value = DiffComputeType.None
                 )

