package africa.absa.cps.analysis

case class RowsDiff(inputAHash: String = "", inputBHash: String = "", diffs: List[ColumnsDiff] = List())
