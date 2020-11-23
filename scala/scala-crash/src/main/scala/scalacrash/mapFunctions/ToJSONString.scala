package scalacrash.mapFunctions

import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write
import org.apache.flink.api.common.functions.MapFunction
import scalacrash.caseClasses.Boat

object ToJSONString extends MapFunction[Boat, String] {
  override def map(value: Boat): String = {
    implicit val formats = DefaultFormats
    write(value)
  }
}
