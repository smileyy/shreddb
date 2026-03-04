package shreddb

import shreddb.storage.{Storage, StorageSystem}

trait ShredConfiguration {
  def getStorageSystem(system: StorageSystem, metadata: Map[String, String]): Storage
}
