package shreddb

trait ShredTable {
  /**
   * The name of the table
   */
  def name: String

  /**
   * Returns the columns in the table
   */
  def columnNames: Seq[String]

  /**
   * Executes the query
   */
  def query(query: ShredQuery): ShredResultSet
}
