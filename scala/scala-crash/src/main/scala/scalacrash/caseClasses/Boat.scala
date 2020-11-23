package scalacrash.caseClasses

case class Boat(
   Name: String,
   Type: String,
   Position: Map[String, Float],
   Velocity: Map[String, Float],
   Orientation: Float,
   Timestamp: Float,
   Colliding: Boolean = false
)
