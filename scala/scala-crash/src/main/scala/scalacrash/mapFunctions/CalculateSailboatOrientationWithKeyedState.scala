package scalacrash.mapFunctions

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import scalacrash.caseClasses.{Boat, Sailboat}

object CalculateSailboatOrientationWithKeyedState extends RichMapFunction[Sailboat, Option[Boat]] {
  private var lastBoat: ValueState[Sailboat] = _

  override def open(parameters: Configuration): Unit = {
    val lastBoatDescriptor = new ValueStateDescriptor[Sailboat]("lastBoat", classOf[Sailboat])
    lastBoat = getRuntimeContext.getState[Sailboat](lastBoatDescriptor)
  }

  override def map(currBoat: Sailboat): Option[Boat] = {
    val r0: Sailboat = lastBoat.value()
    this.lastBoat.update(currBoat)
    if (r0 == null)
      return None
    val r1: Sailboat = currBoat // Most recent
    val vx: Float = (r1.Position("x") - r0.Position("x")) / (r1.Timestamp - r0.Timestamp)
    val vy: Float = (r1.Position("y") - r0.Position("y")) / (r1.Timestamp - r0.Timestamp)
    val velocity = Map("x" -> vx, "y" -> vy)
    val orientation = Math.atan2(vy, vx).toFloat


    if (!vx.isNaN && !vy.isNaN) {
      return Option(Boat(r0.Name, "Sailboat", r0.Position, velocity, orientation, r0.Timestamp))
    }

    None
  }
}