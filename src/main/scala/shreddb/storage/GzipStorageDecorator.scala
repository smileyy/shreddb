package shreddb.storage

import shreddb.ResourceDescriptor

import java.io.{InputStream, OutputStream}
import java.nio.charset.Charset
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

class GzipStorageDecorator(delegate: Storage) extends Storage {

  override def system: StorageSystem = delegate.system

  override def compression: Compression = GzipCompression

  override def newResourceSink(name: String): ResourceSink = new GzipResourceSink(
    delegate.newResourceSink(name)
  )

  override def newResourceSource(descriptor: ResourceDescriptor): ResourceSource = new GzipResourceSource(
    delegate.newResourceSource(descriptor)
  )
}

private class GzipResourceSink(delegate: ResourceSink) extends ResourceSink {
  override def descriptor: ResourceDescriptor = delegate.descriptor
  override def newOutputStream(suffix: String): OutputStream = new GZIPOutputStream(delegate.newOutputStream(suffix))
}

private class GzipResourceSource(delegate: ResourceSource) extends ResourceSource {
  override def newInputStream(suffix: String): InputStream = new GZIPInputStream(delegate.newInputStream(suffix))
}