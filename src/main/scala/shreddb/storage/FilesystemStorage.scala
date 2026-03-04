package shreddb.storage

import shreddb.ResourceDescriptor

import java.io.{InputStream, OutputStream}
import java.nio.charset.Charset
import java.nio.file.{Files, OpenOption, Path, StandardOpenOption}

class FilesystemStorage(val directory: Path) extends Storage {
  override def system: StorageSystem = Filesystem

  override def newResourceSink(name: String): ResourceSink = {
    new FilesystemResourceSink(directory, name)
  }

  override def newResourceSource(descriptor: ResourceDescriptor): ResourceSource = {
    FilesystemResourceSource(descriptor.root.get, descriptor.name)
  }
}

class FilesystemResourceSink(directory: Path, name: String) extends ResourceSink {
  override def descriptor: ResourceDescriptor = ResourceDescriptor(None, Some(directory.toString), name)

  override def newOutputStream(suffix: String): OutputStream = {
    Files.newOutputStream(directory.resolve(name + suffix), StandardOpenOption.CREATE_NEW)
  }
}

class FilesystemResourceSource(directory: Path, name: String) extends ResourceSource {
  override def newInputStream(suffix: String): InputStream = {
    Files.newInputStream(directory.resolve(name + suffix))
  }
}
object FilesystemResourceSource {
  def apply(root: String, name: String): FilesystemResourceSource = {
    new FilesystemResourceSource(Path.of(root), name)
  }
}