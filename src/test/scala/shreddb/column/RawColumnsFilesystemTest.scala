package shreddb.column

import shreddb.storage.{Filesystem, NoCompression}
import shreddb.{ColumnsFormat, ShredRequest, SimpleCsvTestBase}

class RawColumnsFilesystemTest extends SimpleCsvTestBase {
  override def request: ShredRequest = {
    val columns = Seq(
      ColumnDefinition("a", RawStringColumnFormat()),
      ColumnDefinition("b", RawStringColumnFormat()),
      ColumnDefinition("c", RawStringColumnFormat()),
      ColumnDefinition("qty", DecimalColumnFormat)
    )

    ShredRequest(
      input,
      Filesystem,
      columns,
      Some(RawStringColumnFormat()),
      NoCompression,
      ColumnsFormat
    )
  }
}