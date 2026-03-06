package shreddb

import shreddb.storage.{FilesystemStorage, Storage}

import java.nio.file.Path

object TestConfiguration extends ShredConfiguration {
  override protected def getUnderlyingStorage(name: String, metadata: Map[String, String]): Storage = name match {
    case "file" => new FilesystemStorage(Path.of("/tmp/shreddb"))
  }
}
