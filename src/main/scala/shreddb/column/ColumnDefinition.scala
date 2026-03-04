package shreddb.column

import java.nio.charset.{Charset, StandardCharsets}

case class ColumnDefinition(name: String, format: ColumnFormat)

sealed trait ColumnFormat
case object DecimalColumnFormat extends ColumnFormat
case class EnumeratedColumnFormat(representation: EnumerationRepresentation, charset: Charset = StandardCharsets.UTF_8)
    extends ColumnFormat

sealed trait EnumerationRepresentation
case object RawEnumeration extends EnumerationRepresentation
case object TokenizedEnumeration extends EnumerationRepresentation
