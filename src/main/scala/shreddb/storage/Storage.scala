package shreddb.storage

import shreddb.{ResourceDescriptor, StorageDescriptor}

import java.io.{InputStream, OutputStream}

trait Storage {
  /**
   * Creates a new [[ResourceSink]] to write the file or files for a given resource
   */
  def newResourceSink(name: String): ResourceSink

  /**
   * Creates a new [[ResourceSource]] from the descriptor to reade from the "file" with the given name
   */
  def newResourceSource(resource: ResourceDescriptor): ResourceSource
}

trait ResourceSink {
  def descriptor: ResourceDescriptor
  def newOutputStream(suffix: String): OutputStream
}

trait ResourceSource {
  def newInputStream(suffix: String): InputStream
}
