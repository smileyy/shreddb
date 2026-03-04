package shreddb.storage

import java.io.{InputStream, OutputStream}

sealed trait Compression {
  def name: String
}
object Compression {
  private val compressions = Seq(NoCompression, GzipCompression)
  private lazy val compressionsByName: Map[String, Compression] = compressions.map(c => (c.name -> c)).toMap
  
  def apply(name: String): Compression = {
    compressionsByName(name)
  }
}

object NoCompression extends Compression {
  override def name: String = "none"
}

object GzipCompression extends Compression {
  override def name: String = "gzip"
}