package scalacrash
import spray.json._
import DefaultJsonProtocol._

//   private val boatJson = "{\"Name\":\"Tow Me\",\"Position\":{\"x\":0.7600430119040203,\"y\":0.5006601675413694},\"Timestamp\":0.4}"


case class Speedboat (
    name: String,
    position: Map[String, Float],
    velocity: Map[String, Float],
    timestamp: Float
)

object SpeedboatJsonProtocol extends DefaultJsonProtocol {
    implicit val boatFormat: RootJsonFormat[Speedboat] = jsonFormat4(Speedboat)
  }

import SpeedboatJsonProtocol._

object Speedboat {
    def fromJSON(jsonString: String): Option[Speedboat] = {
        val jsonObj = jsonString.parseJson
        val speedboat = jsonObj.convertTo[Speedboat]
        println(speedboat)
    }
}