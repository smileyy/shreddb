package shreddb

import shreddb.column.{ColumnDefinition, ColumnFormat}
import shreddb.input.Input
import shreddb.storage.Storage

trait Shredder {
  def shred(
    name: String,
    storage: Storage,
    input: Input,
    knownColumns: Seq[ColumnDefinition],
    defaultColumnFormat: Option[ColumnFormat]
  ): ShredManifest
}
