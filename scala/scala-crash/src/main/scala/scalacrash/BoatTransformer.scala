package scalacrash

import java.util

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import java.util.ArrayList

import collection.mutable
import scala.collection.JavaConverters._

object BoatTransformer extends  App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.setParallelism(1)

  var speedboatTransformerStream = speedboatTransformer.setupSpeedboatTransformer(env)
  var sailboatTransformerStream = sailboatTransformer.setupSailboatTransform(env)

  var boatPartyStream = speedboatTransformerStream.union(sailboatTransformerStream)

  // use keyed state
  //var boatPartyStreamXfm = transformBoatRichMap(boatPartyStream)

  // use operator state
  var boatPartyStreamXfm = transformBoatRichMapOpState(boatPartyStream)

  boatPartyStreamXfm.print()

  env.execute()

  def transformBoatRichMap(stream: DataStream[Boat]) : DataStream[String] = {
    stream.keyBy(x=>"Boats")
      .map(new BoatCollisionDetectionRichMap)
      .filter(_.isDefined)
      .map(x => Boat.toJSONString(x.get))
  }

  def transformBoatRichMapOpState(stream: DataStream[Boat]) : DataStream[String] = {
    stream
      .map(new BoatCollisionDetectionOpState)
      .filter(_.isDefined)
      .map(x => Boat.toJSONString(x.get))
  }

  class BoatCollisionDetectionRichMap extends RichMapFunction[Boat, Option[Boat]] {
    private var boats: ValueState[Map[String, Boat]] = _

    override def open(parameters: Configuration): Unit = {
      val lastBoatDescriptor = new ValueStateDescriptor[Map[String, Boat]]("boats", classOf[Map[String, Boat]])
      boats = getRuntimeContext.getState[Map[String, Boat]](lastBoatDescriptor)
    }

    override def map(currBoat: Boat): Option[Boat] = {
      println(currBoat.Name)
      val updatedBoats = if (this.boats.value() == null) {
        Map(currBoat.Name->currBoat)
      } else {
        this.boats.value() + (currBoat.Name->currBoat)
      }

      this.boats.update(updatedBoats)

      val firstBoatYouHit = updatedBoats.find(findCollidingBoat(currBoat, _))
      println(firstBoatYouHit)
      Option(currBoat.copy(Colliding=firstBoatYouHit.isDefined))
    }

    def findCollidingBoat(currBoat: Boat, boatData: (String, Boat)): Boolean = {
      if (boatData._1 == currBoat.Name) return false

      Boat.areColliding(currBoat, boatData._2, 10.0)
    }
  }

  class BoatCollisionDetectionOpState
    extends RichMapFunction[Boat, Option[Boat]]
    with ListCheckpointed[java.util.HashMap[String, Boat]] {

    private var boats: java.util.HashMap[String,Boat] = new java.util.HashMap[String,Boat]()

    override def map(currBoat: Boat): Option[Boat] = {
      this.boats.put(currBoat.Name, currBoat)

      val firstBoatYouHit = this.boats.asScala.find(findCollidingBoat(currBoat, _))

      Option(currBoat.copy(Colliding=firstBoatYouHit.isDefined))
    }

    def findCollidingBoat(currBoat: Boat, boatData: (String, Boat)): Boolean = {
      if (boatData._1 == currBoat.Name) return false

      Boat.areColliding(currBoat, boatData._2, 10.0)
    }

    override def restoreState(state: java.util.List[java.util.HashMap[String,Boat]]): Unit = {
      this.boats = new java.util.HashMap[String,Boat]()
      if (state.size() > 0) this.boats = state.get(0)
    }

    override def snapshotState(chkpntId: Long, ts: Long): java.util.List[java.util.HashMap[String,Boat]] = {
      util.Collections.singletonList(this.boats)
    }
  }

}


