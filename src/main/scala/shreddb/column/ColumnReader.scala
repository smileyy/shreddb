package shreddb.column

import shreddb.Criteria

trait ColumnReader {
  def valueAt(idx: Long): Option[Object]
  def filteredBy(criteria: Criteria): FilteredColumnReader
  def close(): Unit
}

trait FilteredColumnReader {
  def nextMatchingIndexAtOrAfter(idx: Long): Option[Long]
}