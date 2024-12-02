package africa.absa.cps.analysis

case class RowsDiff(inputLeftHash: String = "N/A", inputRightHash: String = "N/A", diffs: Seq[ColumnsDiff] = Seq.empty)
