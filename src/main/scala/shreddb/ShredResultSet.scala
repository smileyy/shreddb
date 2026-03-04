package shreddb

class ShredResultSet(val rows: Seq[ShredResultSetRow]) {
  def iterator: Iterator[ShredResultSetRow] = rows.iterator
  def size: Int = rows.size

  override def toString: String = rows.toString()
}

class ShredResultSetRow(val group: Seq[String], val values: Seq[BigDecimal]) {
  override def toString: String = s"$group, $values"
}