package scalacrash.mapFunctions

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import scalacrash.caseClasses.Boat

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

object AreBoatsCollidingWithOperationState extends RichMapFunction[Boat, Boat] with CheckpointedFunction {
  @transient
  private var checkPointedState: ListState[Map[String,Boat]] = _

  private var boats: Map[String,Boat] = Map[String,Boat]()

  override def map(currBoat: Boat): Boat = {
    this.boats = this.boats + (currBoat.Name -> currBoat)

    val firstBoatYouHit = this.boats.find(findCollidingBoat(currBoat, _))

    currBoat.copy(Colliding=firstBoatYouHit.isDefined)
  }

  def findCollidingBoat(currBoat: Boat, boatData: (String, Boat)): Boolean = {
    if (boatData._1 == currBoat.Name) return false

    areColliding(currBoat, boatData._2, 10.0)
  }

  def areColliding(boat1: Boat, boat2: Boat, threshold: Double): Boolean = {
    math.sqrt(math.pow(boat1.Position("x")-boat2.Position("x"),2) + math.pow(boat1.Position("y")-boat2.Position("y"),2)) < threshold
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    val descriptor = new ListStateDescriptor[Map[String, Boat]]("boats",
      TypeInformation.of(new TypeHint[Map[String,Boat]](){})
    )

    checkPointedState = context.getOperatorStateStore.getListState(descriptor)
    if (context.isRestored) {
      this.boats = checkPointedState.get().asScala.head
    }
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    checkPointedState.clear()
    checkPointedState.add(this.boats)
  }
}
