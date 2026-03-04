package shreddb

import shreddb.storage.{Filesystem, FilesystemStorage, Storage, StorageSystem}

import java.nio.file.Path

object TestConfiguration extends ShredConfiguration {
  override def getStorageSystem(system: StorageSystem, metadata: Map[String, String]): Storage = system match {
    case Filesystem =>
      new FilesystemStorage(Path.of("/tmp/shreddb"))
  }
}
