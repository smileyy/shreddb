package shreddb

sealed trait TableFormat {
  def name: String
}
object ColumnsFormat extends TableFormat {
  override def name: String = "column"
}