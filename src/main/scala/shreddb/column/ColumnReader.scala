package shreddb.column

import shreddb.{Criteria, ShredQueryException}

import java.io.{DataInputStream, InputStream}

trait ColumnReader {
  def valueAt(idx: Long): Option[Any]
  def filteredBy(criteria: Criteria): FilteredColumnReader
  def close(): Unit
}

abstract class AbstractColumnReader(numRows: Long) extends ColumnReader {
  private val input = new DataInputStream(newInputStream())

  protected def newInputStream(): InputStream

  var currentIndex: Long = -1
  var currentValue: Any = next()

  override def valueAt(idx: Long): Option[Any] = {
    if (idx == currentIndex) {
      Some(currentValue)
    } else if (idx >= numRows) {
      None
    } else if (currentIndex < idx) {
      while (currentIndex < idx) {
        currentValue = next()
      }
      Some(currentValue)
    } else {
      throw new Exception(s"Cannot read backwards from current index $currentIndex to $idx")
    }
  }

  def next(): Any = {
    currentIndex  = currentIndex + 1
    currentValue = readNext(input)

    currentValue
  }

  protected def readNext(input: DataInputStream): Any

  def hasNext: Boolean = {
    currentIndex < numRows
  }

  override def filteredBy(criteria: Criteria): FilteredColumnReader = {
    throw new ShredQueryException(s"Cannot filter a decimal column (yet!)")
  }

  override def close(): Unit = input.close()
}

trait FilteredColumnReader {
  def nextMatchingIndexAtOrAfter(idx: Long): Option[Long]
}

abstract class AbstractFilteredColumnReader(reader: AbstractColumnReader) extends FilteredColumnReader {
  override def nextMatchingIndexAtOrAfter(idx: Long): Option[Long] = {
    reader.valueAt(idx) match {
      case Some(_) =>
      case None => return None
    }

    var done = false
    var result: Option[Long] = None
    while (!done) {
      if (accepts(reader.currentValue)) {
        result = Some(reader.currentIndex)
        done = true
      } else if (reader.hasNext) {
        reader.next()
      } else {
        done = true
      }
    }

    result
  }

  def accepts(value: Any): Boolean
}