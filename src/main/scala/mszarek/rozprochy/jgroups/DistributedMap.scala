package mszarek.rozprochy.jgroups

import java.io.{DataInputStream, DataOutputStream, InputStream, OutputStream}
import java.net.InetAddress

import com.avsystem.commons.misc.Opt
import org.jgroups._
import com.avsystem.commons._
import com.typesafe.scalalogging.LazyLogging
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.jgroups.protocols._
import org.jgroups.protocols.pbcast.{GMS, NAKACK2, STABLE, STATE_TRANSFER}
import org.jgroups.stack.ProtocolStack
import org.jgroups.util.Util

import scala.collection.mutable.{HashMap => HMap}

class DistributedMap private(jChannel: JChannel) extends ReceiverAdapter with SimpleStringMap with LazyLogging {
  private var map: HMap[String, Int] = HMap.empty

  override def containsKey(key: String): Boolean = {
    for {
      _ <- logTask(s"Local contains for $key")
      res <- mapContainsKey(key)
    } yield res
  }.runSyncUnsafe()

  override def get(key: String): Opt[Int] = {
    for {
      _ <- logTask(s"Get for $key")
      res <- mapGet(key)
    } yield res
  }.runSyncUnsafe()

  override def put(key: String, value: Int): Unit = {
    for {
      _ <- logTask(s"Put for $key -> $value")
      _ <- sendClusterMessage(Put(key, value))
      _ <- mapPut(key, value)
    } yield ()
  }.runSyncUnsafe()

  override def remove(key: String): Opt[Int] = {
    for {
      _ <- logTask(s"Remove for $key")
      _ <- sendClusterMessage(Remove(key))
      res <- mapRemove(key)
    } yield res
  }.runSyncUnsafe()

  override def receive(msg: Message): Unit = {
    for {
      _ <- logTask(s"Received message")
      _ <- msg.getObject match {
        case Put(key, value) => mapPut(key, value)
        case Remove(key) => mapRemove(key)
        case _ => Task.unit
      }
    } yield ()
  }.runSyncUnsafe()

  override def getState(output: OutputStream): Unit = {
    for {
      _ <- logTask("Performing getState")
      _ <- toStream(map, output)
    } yield ()
  }.runSyncUnsafe()

  override def setState(input: InputStream): Unit = {
    for {
      _ <- logTask("Performing setState")
      _ <- Task.eval {
        map = Util.objectFromStream(new DataInputStream(input)).asInstanceOf[HMap[String, Int]]
      }
    } yield ()
  }.runSyncUnsafe()

  override def viewAccepted(view: View): Unit = {
    for {
      _ <- logTask("ViewAccepted")
      _ <- view match {
        case mergeView: MergeView if !mergeView.getSubgroups.get(0).getMembers.contains(jChannel.getAddress) =>
          Task.eval(jChannel.getState(null, 0))
        case _ =>
          Task.unit
      }
    } yield ()
  }.runSyncUnsafe()

  private def toStream(obj: Any, outputStream: OutputStream): Task[Unit] = Task.eval {
    Util.objectToStream(obj, new DataOutputStream(outputStream))
  }

  private def sendClusterMessage(message: ClusterMessage): Task[Unit] = Task.eval {
    jChannel.send(new Message(null, null, message))
  }

  private def mapGet(key: String): Task[Opt[Int]] = Task.eval {
    map.getOpt(key)
  }

  private def mapPut(key: String, value: Int): Task[Unit] = Task.eval {
    map.put(key, value)
  }

  private def mapRemove(key: String): Task[Opt[Int]] = Task.eval {
    map.remove(key).toOpt
  }

  private def mapContainsKey(key: String): Task[Boolean] = Task.eval {
    map.contains(key)
  }

  private def logTask(message: String): Task[Unit] = Task.eval {
    logger.info(message)
  }
}

object DistributedMap {
  def empty(address: String): Task[DistributedMap] = Task.eval {
    val jChannel = new JChannel(false)
    val protocolStack = new ProtocolStack()
      .addProtocol(new UDP().setValue("mcast_group_addr", InetAddress.getByName(address)))
      .addProtocol(new PING())
      .addProtocol(new MERGE3())
      .addProtocol(new FD_SOCK())
      .addProtocol(new FD_ALL().setValue("timeout", 12000).setValue("interval", 3000))
      .addProtocol(new VERIFY_SUSPECT())
      .addProtocol(new BARRIER())
      .addProtocol(new NAKACK2())
      .addProtocol(new UNICAST3())
      .addProtocol(new STABLE())
      .addProtocol(new GMS())
      .addProtocol(new UFC())
      .addProtocol(new MFC())
      .addProtocol(new FRAG2())
      .addProtocol(new STATE_TRANSFER())
    jChannel.setProtocolStack(protocolStack)
    protocolStack.init()
    new DistributedMap(jChannel)
      .setup { map =>
        jChannel.setDiscardOwnMessages(true)
        jChannel.setReceiver(map)
        jChannel.connect("DistributedMap", null, 0)
      }
  }
}