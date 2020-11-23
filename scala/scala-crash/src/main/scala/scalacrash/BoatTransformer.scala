package scalacrash

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

import scala.collection.JavaConverters._

object BoatTransformer extends  App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.setParallelism(1)

  var speedboatTransformerStream = speedboatTransformer.setupSpeedboatTransformer(env)
  var sailboatTransformerStream = sailboatTransformer.setupSailboatTransform(env)

  var boatPartyStream = speedboatTransformerStream.union(sailboatTransformerStream)

  // use keyed state
  //var boatPartyStreamXfm = transformBoatRichMap(boatPartyStream)


  var boatPartyStreamXfm = transformBoatRichMapOpStateNew(boatPartyStream)

  boatPartyStreamXfm.print()

  val kafkaProducer = new FlinkKafkaProducer[String](
    "localhost:9092",
    "boat_data",
    new SimpleStringSchema)
  boatPartyStreamXfm.addSink(kafkaProducer)

  env.execute()

  def transformBoatRichMap(stream: DataStream[Boat]) : DataStream[String] = {
    stream.keyBy(x=>"Boats")
      .map(new BoatCollisionDetectionRichMap)
      .filter(_.isDefined)
      .map(x => Boat.toJSONString(x.get))
  }

  def transformBoatRichMapOpStateNew(stream: DataStream[Boat]) : DataStream[String] = {
    stream
      .map(new BoatCollisionDetectionOpStateNew)
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


  class BoatCollisionDetectionOpStateNew
    extends RichMapFunction[Boat, Option[Boat]]
      with CheckpointedFunction {

    @transient
    private var checkPointedState: ListState[Map[String,Boat]] = _

    private var boats: Map[String,Boat] = Map[String,Boat]()

    override def map(currBoat: Boat): Option[Boat] = {
      this.boats = this.boats + (currBoat.Name -> currBoat)

      val firstBoatYouHit = this.boats.find(findCollidingBoat(currBoat, _))

      Option(currBoat.copy(Colliding=firstBoatYouHit.isDefined))
    }

    def findCollidingBoat(currBoat: Boat, boatData: (String, Boat)): Boolean = {
      if (boatData._1 == currBoat.Name) return false

      Boat.areColliding(currBoat, boatData._2, 10.0)
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

}


