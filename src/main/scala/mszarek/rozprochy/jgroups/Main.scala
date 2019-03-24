package mszarek.rozprochy.jgroups

import scala.util.Try
import monix.execution.Scheduler.Implicits.global

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Main {
  def main(args: Array[String]): Unit = {
    System.setProperty("java.net.preferIPv4Stack", "true")
    val map = DistributedMap.empty(Try(args(0)).getOrElse("230.100.200.1"))
    val commandLineInterface = new CommandLineInterface(map).start().runToFuture
    Await.ready(commandLineInterface, Duration.Inf)
  }
}
