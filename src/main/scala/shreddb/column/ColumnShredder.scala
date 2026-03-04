package shreddb.column

import shreddb.input.Input
import shreddb.storage.Storage
import shreddb.{
  ColumnsFormat,
  ShredManifest,
  Shredder,
  ShreddingException,
  TableDescriptor
}

import java.util.UUID

class ColumnShredder extends Shredder {
  override def shred(name: String, storage: Storage, input: Input, knownColumns: Seq[ColumnDefinition], defaultColumnFormat: Option[ColumnFormat]): ShredManifest = {
    val knownColumnDefinitionsByName = knownColumns.map(definition => definition.name -> definition).toMap

    val writersWithIndex: Seq[(ColumnWriter, Int)] = input.columnNames.map { name => knownColumnDefinitionsByName.get(name) match {
      case Some(definition) => ColumnWriter(storage, name, definition.format)
      case None => defaultColumnFormat match {
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

    ShredManifest(TableDescriptor(name, storage.descriptor, ColumnsFormat, columnDescriptors, numRows))
  }
}
