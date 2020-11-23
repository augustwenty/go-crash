package scalacrash.mapFunctions

import net.liftweb.json.{DefaultFormats, parse}
import org.apache.flink.api.common.functions.MapFunction
import scalacrash.caseClasses.Speedboat

object ToSpeedboat extends MapFunction[String, Speedboat] {
  override def map(value: String): Speedboat = {
    implicit val formats = DefaultFormats
    val jsonObj = parse(value)
    jsonObj.extract[Speedboat]
  }
}
