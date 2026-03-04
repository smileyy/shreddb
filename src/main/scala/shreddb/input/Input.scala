package shreddb.input

trait Input {
  def columnNames: Seq[String]
  def iterator: Iterator[InputRow]
}

trait InputRow {
  def getValue(idx: Int): String
  def getValue(field: String): String
}
