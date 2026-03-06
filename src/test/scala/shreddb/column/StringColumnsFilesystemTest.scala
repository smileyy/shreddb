package shreddb.column

import shreddb.storage.{Compression, NoCompression}
import shreddb.{ColumnsFormat, SimpleCsvTestBase, TableDefinition, TableFormat}

class StringColumnsFilesystemTest extends SimpleCsvTestBase {
  override def format: TableFormat = ColumnsFormat
  override def storage: String = "file"
  override def compression: Compression = NoCompression

  override def columns: Seq[ColumnDefinition] = Seq(
      ColumnDefinition("a", StringColumnFormat()),
      ColumnDefinition("b", StringColumnFormat()),
      ColumnDefinition("c", StringColumnFormat()),
      ColumnDefinition("qty", DecimalColumnFormat)
    )
  
  override def defaultColumnFormat: Option[ColumnFormat] = Some(StringColumnFormat())
}