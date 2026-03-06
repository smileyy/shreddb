package shreddb.column

import shreddb.{Criteria, In, Is, ResourceDescriptor}
import shreddb.storage.{ResourceSink, ResourceSource, Storage}

import java.io.{DataInputStream, DataOutputStream, InputStream}
import java.nio.charset.Charset

class RawStringColumn(val name: String, numRows: Long, storage: Storage, resource: ResourceDescriptor, charset: Charset) extends GroupByColumn {
  override def valueAccessor: GroupByValueAccessor = RawEnumeratedGroupByValueAccessor

  override def newReader(): ColumnReader = new RawStringColumnReader(numRows, storage.newResourceSource(resource), charset)
}

class RawStringColumnWriter(val resource: ResourceSink, val name: String, charset: Charset) extends ColumnWriter {
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

  override def format: ColumnFormat = RawStringColumnFormat(charset)
}

class RawStringColumnReader(val numRows: Long, resource: ResourceSource, charset: Charset) extends AbstractColumnReader(numRows) {
  override protected def newInputStream(): InputStream = resource.newInputStream(".values")

  override protected def readNext(input: DataInputStream): Any = {
    val length = input.readInt()
    val bytes: Array[Byte] = new Array(length)
    input.read(bytes)

    new String(bytes, charset)
  }

  override def filteredBy(criteria: Criteria): FilteredColumnReader = new FilteredRawStringColumnReader(this, criteria)
}

class FilteredRawStringColumnReader(reader: RawStringColumnReader, criteria: Criteria) extends FilteredColumnReader {
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

  def accepts(value: Any): Boolean = {
    criteria match {
      case Is(_, v) => v == value
      case In(_, vs) => vs.contains(value.asInstanceOf[String])
    }
  }
}

object RawEnumeratedGroupByValueAccessor extends GroupByValueAccessor {
  override def getValue(raw: Any): String = raw.asInstanceOf[String]
}