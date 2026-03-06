package shreddb

import shreddb.column.{ColumnDefinition, ColumnFormat, ColumnShredder, ColumnTable}
import shreddb.input.Input
import shreddb.storage.{Compression, NoCompression}

class ShredDb(config: ShredConfiguration) {
  def shred(
    input: Input,
    table: TableDefinition,
    storage: String,
    compression: Compression = NoCompression,
    format: TableFormat = ColumnsFormat,
    metadata: Map[String, String] = Map.empty
  ): ShredManifest = {
    getShredder(format).shred(config, input, table, storage, compression, metadata)
  }

  private def getShredder(format: TableFormat): Shredder = {
    format match {
      case ColumnsFormat => new ColumnShredder()
    }
  }
  
  def getTable(mf: ShredManifest): ShredTable = {
    val tableDescriptor: TableDescriptor = mf.table
    val storage = config.getStorage(tableDescriptor.storage.name, tableDescriptor.storage.compression, mf.metadata)

    tableDescriptor.format match {
      case ColumnsFormat => ColumnTable(storage, tableDescriptor)
    }
  }
}

case class TableDefinition(name: String, columns: Seq[ColumnDefinition], defaultColumnFormat: Option[ColumnFormat] = None)
