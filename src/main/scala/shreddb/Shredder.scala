package shreddb

import shreddb.column.{ColumnDefinition, ColumnFormat}
import shreddb.input.Input
import shreddb.storage.{Compression, Storage}

trait Shredder {
  def shred(
             config: ShredConfiguration,
             input: Input,
             table: TableDefinition,
             storageName: String,
             compression: Compression,
             metadata: Map[String, String]
           ): ShredManifest
}
