package shreddb.column

import shreddb.storage.{ResourceSink, Storage}
import shreddb.ColumnDescriptor

import java.util.UUID

trait ColumnWriter {
  def name: String
  def format: ColumnFormat
  def resource: ResourceSink

  def addValue(value: String): Unit
  def close(): Unit

  def descriptor: ColumnDescriptor = ColumnDescriptor(name, format, Some(resource.descriptor))
}

object ColumnWriter {
  def apply(storage: Storage, name: String, format: ColumnFormat): ColumnWriter = {
    val sink = storage.newResourceSink(UUID.randomUUID().toString)

    format match {
      case RawStringColumnFormat(charset) => new RawStringColumnWriter(sink, name, charset)
      case TokenizedStringColumnFormat(charset) => new TokenizedStringColumnWriter(sink, name, charset)
      case DecimalColumnFormat => new DecimalColumnWriter(sink, name)
    }
  }
}