package mszarek.rozprochy.jgroups

import com.avsystem.commons.misc.Opt

trait SimpleStringMap {
  def containsKey(key: String): Boolean
  def get(key: String): Opt[Int]
  def put(key: String, value: Int): Unit
  def remove(key: String): Opt[Int]
}
