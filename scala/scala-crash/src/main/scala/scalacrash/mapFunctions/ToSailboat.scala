package scalacrash.mapFunctions

import net.liftweb.json.{DefaultFormats, parse}
import org.apache.flink.api.common.functions.MapFunction
import scalacrash.caseClasses.Sailboat

object ToSailboat extends MapFunction[String, Sailboat] {
    override def map(value: String): Sailboat = {
      implicit val formats = DefaultFormats
      val jsonObj = parse(value)
      jsonObj.extract[Sailboat]
    }
}
