package shreddb.column

import java.nio.charset.{Charset, StandardCharsets}

case class ColumnDefinition(name: String, format: ColumnFormat)

sealed trait ColumnFormat
case class RawStringColumnFormat(charset: Charset = StandardCharsets.UTF_8) extends ColumnFormat
case class TokenizedStringColumnFormat(charset: Charset = StandardCharsets.UTF_8) extends ColumnFormat
case object DecimalColumnFormat extends ColumnFormat