package shreddb.column

import shreddb.{Criteria, In, Is, ResourceDescriptor}
import shreddb.storage.{ResourceSink, ResourceSource, Storage}

import java.io.{DataInputStream, DataOutputStream}
import java.nio.charset.Charset

class RawEnumeratedColumn(val name: String, numRows: Long, storage: Storage, resource: ResourceDescriptor, charset: Charset) extends GroupByColumn {
  override def valueAccessor: GroupByValueAccessor = RawEnumeratedGroupByValueAccessor

  override def newReader(): ColumnReader = new RawEnumeratedColumnReader(numRows, storage.newResourceSource(resource), charset)
}

class RawEnumeratedColumnWriter(val resource: ResourceSink, val name: String, charset: Charset) extends ColumnWriter {
  private val output = new DataOutputStream(resource.newOutputStream(".values"))

  override def addValue(value: String): Unit = {
    val bytes = value.getBytes(charset)
    val length = bytes.length

    output.writeInt(length)
    output.write(bytes)
  }

  override def close(): Unit = {
    output.close()
  }

  override def format: ColumnFormat = EnumeratedColumnFormat(RawEnumeration, charset)
}

class RawEnumeratedColumnReader(val numRows: Long, resource: ResourceSource, charset: Charset) extends ColumnReader {
  private val input = new DataInputStream(resource.newInputStream(".values"))

  var currentIndex = -1
  var currentValue: String = next()

  override def valueAt(idx: Long): Option[Object] = {
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

  def next(): String = {
      val length = input.readInt()
      val bytes: Array[Byte] = new Array(length)
      input.read(bytes)
      val result = new String(bytes, charset)

      currentIndex = currentIndex + 1
      currentValue = result

      currentValue
  }

  def hasNext: Boolean = {
    currentIndex < numRows
  }

  override def filteredBy(criteria: Criteria): FilteredColumnReader = new FilteredRawEnumeratedColumnReader(this, criteria)

  override def close(): Unit = input.close()
}

class FilteredRawEnumeratedColumnReader(reader: RawEnumeratedColumnReader, criteria: Criteria) extends FilteredColumnReader {
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

  def accepts(value: String): Boolean = {
    criteria match {
      case Is(_, v) => v == value
      case In(_, vs) => vs.contains(value)
    }
  }
}

object RawEnumeratedGroupByValueAccessor extends GroupByValueAccessor {
  override def getValue(raw: Object): String = raw.asInstanceOf[String]
}