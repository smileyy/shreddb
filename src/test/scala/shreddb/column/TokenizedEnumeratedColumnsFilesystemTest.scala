package shreddb.column

import shreddb.*
import shreddb.column.{ColumnDefinition, DecimalColumnFormat, EnumeratedColumnFormat, TokenizedEnumeration}
import shreddb.storage.*

class TokenizedEnumeratedColumnsFilesystemTest extends SimpleCsvTestBase {
  override def request: ShredRequest = {
    val columns = Seq(
      ColumnDefinition("a", EnumeratedColumnFormat(TokenizedEnumeration)),
      ColumnDefinition("b", EnumeratedColumnFormat(TokenizedEnumeration)),
      ColumnDefinition("c", EnumeratedColumnFormat(TokenizedEnumeration)),
      ColumnDefinition("qty", DecimalColumnFormat)
    )

    ShredRequest(
      input,
      Filesystem,
      columns,
      Some(EnumeratedColumnFormat(TokenizedEnumeration)),
      NoCompression,
      ColumnsFormat
    )
  }
}