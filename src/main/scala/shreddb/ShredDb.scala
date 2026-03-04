package shreddb

import shreddb.column.{ColumnDefinition, ColumnFormat, ColumnShredder, ColumnTable}
import shreddb.input.Input
import shreddb.storage.{Compression, GzipCompression, GzipStorageDecorator, NoCompression, Storage, StorageSystem}

class ShredDb(config: ShredConfiguration) {
  def shred(
    name: String,
    input: Input,
    storageSystem: StorageSystem,
    columns: Seq[ColumnDefinition],
    defaultColumnFormat: Option[ColumnFormat] = None,
    compression: Compression = NoCompression,
    format: TableFormat = ColumnsFormat,
    metadata: Map[String, String] = Map.empty
  ): ShredManifest = {
    val storage = config.getStorageSystem(storageSystem, metadata)
    val shredder = getShredder(name, format)
    val mf = shredder.shred(name, storage, input, columns, defaultColumnFormat)
    mf.withMetadata(metadata)
  }

  private def getShredder(name: String, format: TableFormat): Shredder = {
    format match {
      case ColumnsFormat => new ColumnShredder()
    }
  }

  def getTable(mf: ShredManifest): ShredTable = {
    val tableDescriptor: TableDescriptor = mf.table

    val storage = getStorage(tableDescriptor.storage.system, tableDescriptor.storage.compression, mf.metadata)

    tableDescriptor.format match {
      case ColumnsFormat => ColumnTable(storage, tableDescriptor)
    }
  }

  private def getStorage(system: StorageSystem, compression: Compression, metadata: Map[String, String]) = {
    val underlyingStorage: Storage = config.getStorageSystem(system, metadata)
    val storage = compression match {
      case NoCompression => underlyingStorage
      case GzipCompression => new GzipStorageDecorator(underlyingStorage)
    }
    storage
  }
}