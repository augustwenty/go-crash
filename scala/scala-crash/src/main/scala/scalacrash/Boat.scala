package scalacrash

import net.liftweb.json._
import net.liftweb.json.JsonDSL._
import net.liftweb.json.JsonAST._
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

    def toJSONStringBrutish(boat: Boat): String = {
        val jsonString = s"""{"Name":"${boat.Name}","Type":"${boat.Type}","Position":{"x":${boat.Position("x")},"y":${boat.Position("y")}},"Velocity":{"x":${boat.Velocity("x")},"y":${boat.Velocity("y")}},"Orientation":${boat.Orientation},"Timestamp":${boat.Timestamp}}"""
        jsonString
    }
}