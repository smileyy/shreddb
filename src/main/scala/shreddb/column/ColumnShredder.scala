package shreddb.column

import shreddb.input.Input
import shreddb.storage.{Compression, Storage}
import shreddb.{ColumnsFormat, ShredConfiguration, ShredManifest, Shredder, ShreddingException, StorageDescriptor, TableDefinition, TableDescriptor}

import java.util.UUID

class ColumnShredder extends Shredder {
  override def shred(config: ShredConfiguration, input: Input, table: TableDefinition, storageName: String, compression: Compression, metadata: Map[String, String]): ShredManifest = {
    val storage = config.getStorage(storageName, compression, metadata)

    val knownColumnDefinitionsByName = table.columns.map(definition => definition.name -> definition).toMap

    val writersWithIndex: Seq[(ColumnWriter, Int)] = input.columnNames.map { name => knownColumnDefinitionsByName.get(name) match {
      case Some(definition) => ColumnWriter(storage, name, definition.format)
      case None => table.defaultColumnFormat match {
        case Some(format) => ColumnWriter(storage, name, format)
        case None => throw new ShreddingException(s"Unknown column $name with no default format provided")
      }
    }}.zipWithIndex

    var numRows = 0
    input.iterator.foreach { row =>
      numRows = numRows + 1
      writersWithIndex.foreach { (writer, idx) =>
        writer.addValue(row.getValue(idx))
      }
    }

    writersWithIndex.foreach { (writer, _) =>
      writer.close()
    }

    val columnDescriptors = writersWithIndex.map { (writer, _) =>
      writer.descriptor
    }

    ShredManifest(TableDescriptor(table.name, StorageDescriptor(storageName, compression), ColumnsFormat, columnDescriptors, numRows), metadata)
  }
}
