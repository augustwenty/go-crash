package scalacrash

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import scalacrash.sailboatTransformer.SailboatToBoatTransformRichMap

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
      .filter(x => {
        x.isDefined
      })
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

      val firstBoatYouHit = this.boats.value().find(boat => {
        if (boat._1 != currBoat.Name) {
          if (Boat.areColliding(currBoat, boat._2, 10)) {
            return Option(boat._2)
          }
        }
        return None
      } )

      val didCollide = firstBoatYouHit.isDefined
      Option(currBoat.copy(Colliding=didCollide))
    }

  }
}


