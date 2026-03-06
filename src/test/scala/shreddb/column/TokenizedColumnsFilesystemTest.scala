package shreddb.column

import shreddb.storage.{Compression, NoCompression}
import shreddb.{ColumnsFormat, SimpleCsvTestBase, TableDefinition, TableFormat}

class TokenizedColumnsFilesystemTest extends SimpleCsvTestBase {
  override def format: TableFormat = ColumnsFormat
  override def storage: String = "file"
  override def compression: Compression = NoCompression

  override def columns: Seq[ColumnDefinition] = Seq(
    ColumnDefinition("a", TokenizedStringColumnFormat()),
    ColumnDefinition("b", TokenizedStringColumnFormat()),
    ColumnDefinition("c", TokenizedStringColumnFormat()),
    ColumnDefinition("qty", DecimalColumnFormat)
  )
  override def defaultColumnFormat: Option[ColumnFormat] = Some(TokenizedStringColumnFormat())
}