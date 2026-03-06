package shreddb.column

import shreddb.{Criteria, ResourceDescriptor, ShredQueryException}
import shreddb.storage.{ResourceSink, ResourceSource, Storage}

import java.io.{DataInputStream, DataOutputStream, InputStream}
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

class DecimalColumnReader(numRows: Long, resource: ResourceSource) extends AbstractColumnReader(numRows) {
  override def newInputStream(): InputStream = resource.newInputStream(".values")


  override protected def readNext(input: DataInputStream): Any = {
    val scale = input.readInt()

    val length = input.readInt()
    val bytes: Array[Byte] = new Array(length)
    input.read(bytes)

    val integer = new BigInteger(bytes)
    new BigDecimal(new java.math.BigDecimal(integer, scale))
  }
}