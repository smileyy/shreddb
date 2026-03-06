package shreddb.column

import shreddb.storage.{Filesystem, NoCompression}
import shreddb.{ColumnsFormat, ShredRequest, SimpleCsvTestBase}

class StringColumnsFilesystemTest extends SimpleCsvTestBase {
  override def request: ShredRequest = {
    val columns = Seq(
      ColumnDefinition("a", StringColumnFormat()),
      ColumnDefinition("b", StringColumnFormat()),
      ColumnDefinition("c", StringColumnFormat()),
      ColumnDefinition("qty", DecimalColumnFormat)
    )

    ShredRequest(
      input,
      Filesystem,
      columns,
      Some(StringColumnFormat()),
      NoCompression,
      ColumnsFormat
    )
  }
}