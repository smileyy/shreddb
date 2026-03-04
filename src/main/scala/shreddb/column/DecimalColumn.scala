package shreddb.column

import shreddb.{Criteria, ResourceDescriptor, ShredQueryException}
import shreddb.storage.{ResourceSink, ResourceSource, Storage}

import java.io.{DataInputStream, DataOutputStream}
import java.math.BigInteger

class DecimalColumn(val name: String, numRows: Long, storage: Storage, resource: ResourceDescriptor) extends Column {
  override def newReader(): ColumnReader = new DecimalColumnReader(numRows, storage.newResourceSource(resource))
}

class DecimalColumnWriter(val resource: ResourceSink, val name: String) extends ColumnWriter {
  private val output = new DataOutputStream(resource.newOutputStream(".values"))

  override def addValue(value: String): Unit = {
    val javaDecimal = BigDecimal(value).bigDecimal

    val scale: Int = javaDecimal.scale()
    val integerBytes: Array[Byte] = javaDecimal.toBigInteger.toByteArray
    val length: Int = integerBytes.length

    output.writeInt(scale)
    output.writeInt(length)
    output.write(integerBytes)
  }

  override def close(): Unit = {
    output.close()
  }

  override def format: ColumnFormat = DecimalColumnFormat
}

class DecimalColumnReader(numRows: Long, resource: ResourceSource) extends ColumnReader {
  private val input = new DataInputStream(resource.newInputStream(".values"))

  var currentIndex: Long = -1
  var currentValue: BigDecimal = next()

  override def valueAt(idx: Long): Option[Object] = {
    if (idx == currentIndex) {
      Some(currentValue)
    } else if (currentIndex < idx) {
      while (currentIndex < idx) {
        currentValue = next()
      }
      Some(currentValue)
    } else if (idx >= numRows) {
      None
    } else {
      throw new Exception(s"Cannot read backwards from current index $currentIndex to $idx")
    }
  }

  def next(): BigDecimal = {
    val scale = input.readInt()
    val length = input.readInt()
    val bytes: Array[Byte] = new Array(length)
    input.read(bytes)

    val integer = new BigInteger(bytes)
    val result = new java.math.BigDecimal(integer, scale)

    currentIndex  = currentIndex + 1
    
    result
  }

  def hasNext: Boolean = {
    currentIndex < numRows
  }

  override def filteredBy(criteria: Criteria): FilteredColumnReader = {
    throw new ShredQueryException(s"Cannot filter a decimal column (yet!)")
  }

  override def close(): Unit = input.close()
}