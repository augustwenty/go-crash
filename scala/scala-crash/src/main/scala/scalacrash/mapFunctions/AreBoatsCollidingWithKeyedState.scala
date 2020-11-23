package scalacrash.mapFunctions

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import scalacrash.caseClasses.Boat

object AreBoatsCollidingWithKeyedState extends RichMapFunction[Boat, Boat] {
  private var boats: ValueState[Map[String, Boat]] = _

  override def open(parameters: Configuration): Unit = {
    val lastBoatDescriptor = new ValueStateDescriptor[Map[String, Boat]]("boats", classOf[Map[String, Boat]])
    boats = getRuntimeContext.getState[Map[String, Boat]](lastBoatDescriptor)
  }

  override def map(currBoat: Boat): Boat = {
    println(currBoat.Name)
    val updatedBoats = if (this.boats.value() == null) {
      Map(currBoat.Name->currBoat)
    } else {
      this.boats.value() + (currBoat.Name->currBoat)
    }

    this.boats.update(updatedBoats)

    val firstBoatYouHit = updatedBoats.find(findCollidingBoat(currBoat, _))
    println(firstBoatYouHit)
    currBoat.copy(Colliding=firstBoatYouHit.isDefined)
  }

  def findCollidingBoat(currBoat: Boat, boatData: (String, Boat)): Boolean = {
    if (boatData._1 == currBoat.Name) return false

    areColliding(currBoat, boatData._2, 10.0)
  }

  def areColliding(boat1: Boat, boat2: Boat, threshold: Double): Boolean = {
    math.sqrt(math.pow(boat1.Position("x")-boat2.Position("x"),2) + math.pow(boat1.Position("y")-boat2.Position("y"),2)) < threshold
  }
}