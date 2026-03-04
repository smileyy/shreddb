package shreddb.storage

import shreddb.{ResourceDescriptor, StorageDescriptor}

import java.io.{InputStream, OutputStream}
import java.nio.charset.Charset

trait Storage {
  /**
   * The type of [[StorageSystem]] this is.
   */
  def system: StorageSystem

  /**
   * Indicates the type of compression being used by this system. This method is intended to be overwritten by 
   * compression decorators such as [[GzipStorageDecorator]]; it should not be overridden by direct implementations. 
   */
  def compression: Compression = NoCompression

  /**
   * Creates a descriptor for inclusion in a [[shreddb.ShredManifest]] to describe how a table is written to this
   * storage system.
   */
  final def descriptor: StorageDescriptor = StorageDescriptor(system, compression)

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
