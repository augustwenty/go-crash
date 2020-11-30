package scalacrash.mapFunctions

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import scalacrash.caseClasses.Boat

class BoatDedupe extends RichFlatMapFunction[Boat, Boat] {
  private var keyCache: List[String] = List[String]()

  override def flatMap(boat: Boat, out: Collector[Boat]): Unit = {
    val currKey = s"${boat.Name}-${math.round(boat.Timestamp*1000).toLong}-${boat.Colliding}"
    if (!keyCache.contains(currKey))
    {
      keyCache = currKey :: keyCache
      if (keyCache.size >= 100) {
        keyCache = keyCache.init
      }
      out.collect(boat)
    }
  }
}
