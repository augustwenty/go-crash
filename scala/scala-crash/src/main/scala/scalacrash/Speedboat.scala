package scalacrash
import net.liftweb.json._
import org.apache.flink.api.common.functions.MapFunction

case class Speedboat (
    Name: String,
    Position: Map[String, Float],
    Velocity: Map[String, Float],
    Timestamp: Float
)

object Speedboat extends MapFunction[String, Speedboat] {
    override def map(value: String): Speedboat = {
        implicit val formats = DefaultFormats
        val jsonObj = parse(value)
        jsonObj.extract[Speedboat]
    }
}