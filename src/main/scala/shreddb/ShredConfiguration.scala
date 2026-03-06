package shreddb

import shreddb.storage.{Compression, GzipCompression, GzipStorageDecorator, NoCompression, Storage}

trait ShredConfiguration {
  def getStorage(name: String, compression: Compression, metadata: Map[String, String]): Storage = {
    val underlying: Storage = getUnderlyingStorage(name, metadata)
    compression match {
      case NoCompression => underlying
      case GzipCompression => new GzipStorageDecorator(underlying)
    }
  }
  
  protected def getUnderlyingStorage(name: String, metadata: Map[String, String]): Storage
}
