package scalacrash

import net.liftweb.json.{DefaultFormats, parse}
import org.apache.flink.api.common.functions.MapFunction

case class Sailboat (
  Name: String,
  Position: Map[String, Float],
  Timestamp: Float
)

object Sailboat extends MapFunction[String, Sailboat] {
  override def map(value: String): Sailboat = {
    implicit val formats = DefaultFormats
    val jsonObj = parse(value)
    jsonObj.extract[Sailboat]
  }
}