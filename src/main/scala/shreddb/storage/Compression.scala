package shreddb.storage

sealed trait Compression {
  def name: String
}

object NoCompression extends Compression {
  override def name: String = "none"
}

object GzipCompression extends Compression {
  override def name: String = "gzip"
}