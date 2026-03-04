package shreddb.column

import shreddb.storage.{Filesystem, NoCompression}
import shreddb.{ColumnsFormat, ShredRequest, SimpleCsvTestBase}

class TokenizedColumnsFilesystemTest extends SimpleCsvTestBase {
  override def request: ShredRequest = {
    val columns = Seq(
      ColumnDefinition("a", TokenizedStringColumnFormat()),
      ColumnDefinition("b", TokenizedStringColumnFormat()),
      ColumnDefinition("c", TokenizedStringColumnFormat()),
      ColumnDefinition("qty", DecimalColumnFormat)
    )

    ShredRequest(
      input,
      Filesystem,
      columns,
      Some(TokenizedStringColumnFormat()),
      NoCompression,
      ColumnsFormat
    )
  }
}