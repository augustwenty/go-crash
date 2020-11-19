package scalacrash

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object BoatTransformer extends  App {
//  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val env = StreamExecutionEnvironment.createLocalEnvironment()

  var speedboatTransformerStream = speedboatTransformer.setupSpeedboatTransformer(env)
  var sailboatTransformerStream = sailboatTransformer.setupSailboatTransform(env)

  var boatPartyStream = speedboatTransformerStream.union(sailboatTransformerStream)
  var boatPartyStreamXfm = transformBoatRichMap(boatPartyStream)

  boatPartyStreamXfm.print()

  env.execute()

  def transformBoatRichMap(stream: DataStream[Boat]) : DataStream[String] = {
    stream.map(new BoatCollisionDetectionRichMap)
      .filter(_.isDefined)
      .map(x => Boat.toJSONString(x.get))
  }

  class BoatCollisionDetectionRichMap extends RichMapFunction[Boat, Option[Boat]] {
    private var boats: ValueState[Map[String, Boat]] = _

    override def open(parameters: Configuration): Unit = {
//      super.open(parameters)
      val lastBoatDescriptor = new ValueStateDescriptor[Map[String, Boat]]("boats", classOf[Map[String, Boat]])
      boats = getRuntimeContext.getState[Map[String, Boat]](lastBoatDescriptor)
      println("YO FOOL!")
    }

    override def map(currBoat: Boat): Option[Boat] = {
      val updatedBoats = this.boats.value() + (currBoat.Name->currBoat)
      this.boats.update(updatedBoats)

      val firstBoatYouHit = this.boats.value().find(findCollidingBoat(currBoat, _))

      Option(currBoat.copy(Colliding=firstBoatYouHit.isDefined))
    }

    def findCollidingBoat(currBoat: Boat, boatData: (String, Boat)): Boolean = {
      if (boatData._1 == currBoat.Name) return false

      Boat.areColliding(currBoat, boatData._2, 10)
    }
  }
}


