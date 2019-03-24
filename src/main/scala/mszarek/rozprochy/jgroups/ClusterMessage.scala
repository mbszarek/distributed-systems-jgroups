package mszarek.rozprochy.jgroups

trait ClusterMessage {
  def key: String
}

case class Put(key: String, value: Integer) extends ClusterMessage
case class Remove(key: String) extends ClusterMessage