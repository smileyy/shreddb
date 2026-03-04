package shreddb.storage

sealed trait StorageSystem {
  def name: String
}
object Filesystem extends StorageSystem {
  override def name: String = "filesystem"
}
