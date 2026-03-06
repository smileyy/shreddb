package shreddb.column

trait GroupByValueAccessor {
  def getValue(raw: Any): String
}
