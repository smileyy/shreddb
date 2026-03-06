package shreddb

import shreddb.column.ColumnFormat
import shreddb.storage.Compression

case class ShredManifest(table: TableDescriptor, metadata: Map[String, String])

case class TableDescriptor(
    name: String,
    storage: StorageDescriptor,
    format: TableFormat,
    columns: Seq[ColumnDescriptor],
    numRows: Long,
    resource: Seq[ResourceDescriptor] = Seq.empty
) {
  lazy val columnsByName: Map[String, ColumnDescriptor] = {
    columns.map { c => c.name -> c }.toMap
  }
}

case class StorageDescriptor(name: String, compression: Compression)
case class ColumnDescriptor(name: String, format: ColumnFormat, resource: Option[ResourceDescriptor])
case class ResourceDescriptor(container: Option[String], root: Option[String], name: String)