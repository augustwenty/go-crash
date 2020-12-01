package scalacrash.flatMapFunctions

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector
import scalacrash.caseClasses.Boat

class AreBoatsCollidingFlatMap(var eps: Float, var timeout: Float) extends RichFlatMapFunction[Boat, Boat] {
    private var boatHistory: Map[String, Boat] = Map[String, Boat]()
//    private val eps = 0.001F
//    private val timeout = .5F

    def flatMap(currBoat: Boat, out: Collector[Boat]): Unit = {
      this.boatHistory = this.boatHistory + (currBoat.Name -> currBoat)
      val currTime = currBoat.Timestamp


      if (currTime > 0) {
        val temporalCount = findCoexistingBoatKeys(currTime, eps).size

        if (temporalCount == this.boatHistory.keys.size) {
          boatHistory.foreach(bh => {
            val firstBoatHit = boatHistory.find(findCollidingBoats(bh._2, _))
            out.collect(bh._2.copy(Colliding=firstBoatHit.isDefined))
          })
        } else {
          boatHistory --= findTimedoutBoatKeys(currTime, timeout)
        }
      }
    }

    def findCoexistingBoatKeys(eventTime: Float, threshold: Float): List[String] = {
      boatHistory.filter{ case (_,value) => math.abs(value.Timestamp-eventTime) < threshold }.keys.toList
    }

    def findTimedoutBoatKeys(eventTime: Float, threshold: Float): List[String] = {
      boatHistory.filter{ case (_,value) => math.abs(value.Timestamp-eventTime) >= threshold }.keys.toList
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
