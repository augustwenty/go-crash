package scalacrash
import net.liftweb.json._

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
        jsonObj.extract[Speedboat]
    }
}