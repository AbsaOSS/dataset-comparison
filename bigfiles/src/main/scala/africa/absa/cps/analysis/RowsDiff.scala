package africa.absa.cps.analysis

case class RowsDiff(inputAHash: String = "N/A", inputBHash: String = "N/A", diffs: Seq[ColumnsDiff] = Seq.empty)
