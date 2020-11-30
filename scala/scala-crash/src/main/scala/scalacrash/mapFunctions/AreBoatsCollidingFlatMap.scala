package scalacrash.mapFunctions

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector
import scalacrash.caseClasses.Boat

class AreBoatsCollidingFlatMap extends RichFlatMapFunction[Boat, Boat] {
    private var boatHistory: Map[String, Boat] = Map[String, Boat]()
    private val eps = 0.001

    def flatMap(currBoat: Boat, out: Collector[Boat]): Unit = {
      this.boatHistory = this.boatHistory + (currBoat.Name -> currBoat)

      if (currBoat.Timestamp > 0) {
        val temporalCount = boatHistory.filter{ case (key,value) => math.abs(value.Timestamp-currBoat.Timestamp) < eps }.keys.size

        if (temporalCount == this.boatHistory.keys.size) {
          boatHistory.foreach(bmap => {
            val firstBoatHit = boatHistory.find(findCollidingBoats(bmap._2, _))
            out.collect(bmap._2.copy(Colliding=firstBoatHit.isDefined))
          })
        }
      }
    }

    def findCollidingBoats(currBoat: Boat, boatData: (String, Boat)): Boolean = {
        val dt = math.abs(boatData._2.Timestamp-currBoat.Timestamp)
        if (boatData._1 == currBoat.Name || (dt > eps)) return false
        areColliding(currBoat, boatData._2, 10.0)
    }

    def areColliding(boat1: Boat, boat2: Boat, threshold: Double): Boolean = {
      math.sqrt(math.pow(boat1.Position("x")-boat2.Position("x"),2) + math.pow(boat1.Position("y")-boat2.Position("y"),2)) < threshold
    }
}
