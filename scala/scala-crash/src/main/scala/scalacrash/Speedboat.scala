package scalacrash
import net.liftweb.json._

//   private val boatJson = "{\"Name\":\"Tow Me\",\"Position\":{\"x\":0.7600430119040203,\"y\":0.5006601675413694},\"Timestamp\":0.4}"


case class Speedboat (
    Name: String,
    Position: Map[String, Float],
    Velocity: Map[String, Float],
    Timestamp: Float
)


object Speedboat {
    def fromJSON(jsonString: String): Speedboat = {
        implicit val formats = DefaultFormats
        val jsonObj = parse(jsonString)
        val speedboat = jsonObj.extract[Speedboat]
        println(speedboat)
        speedboat
    }
}