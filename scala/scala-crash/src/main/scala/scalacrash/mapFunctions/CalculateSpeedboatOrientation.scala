package scalacrash.mapFunctions

import org.apache.flink.api.common.functions.MapFunction
import scalacrash.caseClasses.{Boat, Speedboat}

object CalculateSpeedboatOrientation extends MapFunction[Speedboat, Boat] {
  override def map(value: Speedboat): Boat = {
    val orientation: Float = Math.atan2(value.Velocity("y"), value.Velocity("x")).toFloat
    Boat(value.Name, "speedboat", value.Position, value.Velocity,
      orientation, value.Timestamp)
  }
}
