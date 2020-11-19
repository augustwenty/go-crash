package scalacrash

import net.liftweb.json._
import net.liftweb.json.Serialization.write

case class Boat (
    Name: String,
    Type: String,
    Position: Map[String, Float],
    Velocity: Map[String, Float],
    Orientation: Float,
    Timestamp: Float,
    Colliding: Boolean = false
)


object Boat {
    def transform(speedboat: Speedboat): Boat = {
        val orientation: Float = Math.atan2(speedboat.Velocity("y"), speedboat.Velocity("x")).toFloat
        Boat(speedboat.Name, "speedboat", speedboat.Position, speedboat.Velocity, 
                orientation, speedboat.Timestamp)
    }

    def transform(sailboat: Sailboat): Boat = {
        val vel  = Map("x"->1F, "y"->2F)
        val orientation: Float = 1.0F //Float = Math.atan2(sailboat.Velocity("y"), sailboat.Velocity("x")).toFloat
        Boat(sailboat.Name, "sailboat", sailboat.Position, vel,
            orientation, sailboat.Timestamp)
    }

    def areColliding(boat1: Boat, boat2: Boat, threshold: Double): Boolean = {
        math.sqrt(math.pow(boat1.Position("x")-boat2.Position("x"),2) + math.pow(boat1.Position("y")-boat2.Position("y"),2)) < threshold
    }

    def toJSONString(boat: Boat): String = {
        implicit val formats = DefaultFormats
        write(boat)
    }
}