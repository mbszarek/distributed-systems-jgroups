package mszarek.rozprochy.jgroups

import com.avsystem.commons.misc.Opt
import monix.eval.Task

import scala.io.StdIn
import scala.util.matching.Regex

class CommandLineInterface(map: DistributedMap) {
  def start(): Task[Unit] = for {
    _ <- loop()
  } yield ()

  private def loop(): Task[Unit] = for {
    line <- readLine()
    _ <- matchLine(line)
    _ <- loop()
  } yield ()

  private def readLine(): Task[String] = Task.eval {
    print("prompt> ")
    StdIn.readLine()
  }

  private def matchLine(line: String): Task[Unit] = Task.eval {
    import CommandLineInterface._
    line match {
      case Get(key) =>
        map.get(key) match {
          case Opt(value) =>
            println(s"Entry: $key -> $value")
          case _ =>
            println(s"Entry with key: $key does not exist")
        }
      case Put(key, value) =>
        println(s"Putting entry $key -> $value to map.")
        map.put(key, value.toInt)
      case Contains(key) =>
        if (map.containsKey(key))
          println(s"Entry with key: $key exists in map")
        else
          println(s"Entry with key: $key does not exists")
      case Remove(key) =>
        map.remove(key) match {
          case Opt(_) =>
            println("Removed element from map")
          case Opt.Empty =>
            println("Entry did not exist in map")
        }
      case _ =>
        println("Unknown command.")
    }
  }
}

object CommandLineInterface {
  val Get: Regex = """^get '(.*)'$""".r
  val Put: Regex = """^put '(.*)' ([0-9]+)$""".r
  val Contains: Regex = """^contains '(.*)'""".r
  val Remove: Regex = """^remove '(.*)'$""".r
}
