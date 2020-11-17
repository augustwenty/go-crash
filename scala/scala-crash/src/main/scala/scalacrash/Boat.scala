package scalacrash

import net.liftweb.json._
import net.liftweb.json.Serialization.write

case class Boat (
    Name: String,
    Type: String,
    Position: Map[String, Float],
    Velocity: Map[String, Float],
    Orientation: Float,
    Timestamp: Float
)


object Boat {
    def transform(speedboat: Speedboat): Boat = {
        val orientation: Float = Math.atan2(speedboat.Velocity("y"), speedboat.Velocity("x")).toFloat
        Boat(speedboat.Name, "speedboat", speedboat.Position, speedboat.Velocity, 
                orientation, speedboat.Timestamp)
    }

    def transform(sailboat: Sailboat): Boat = {
        val orientation: Float = Math.atan2(sailboat.Velocity("y"), sailboat.Velocity("x")).toFloat
        Boat(sailboat.Name, "speedboat", sailboat.Position, sailboat.Velocity,
            orientation, sailboat.Timestamp)
    }

    def toJSONString(boat: Boat): String = {
        implicit val formats = DefaultFormats
        write(boat)
    }
}