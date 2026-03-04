package shreddb.column

trait Column {
  def name: String
  def newReader(): ColumnReader
}

trait GroupByColumn extends Column {
  def valueAccessor: GroupByValueAccessor
}