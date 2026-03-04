package shreddb.column

trait GroupByValueAccessor {
  def getValue(raw: Object): String
}
