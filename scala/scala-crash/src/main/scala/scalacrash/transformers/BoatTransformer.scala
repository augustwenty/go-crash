package scalacrash.transformers

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import scalacrash.caseClasses.Boat
import scalacrash.mapFunctions.{AreBoatsCollidingWithKeyedState, AreBoatsCollidingWithOperationState, ToJSONString}

object BoatTransformer extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.setParallelism(1)

  var speedboatTransformerStream = speedboatTransformer.setupSpeedboatTransformer(env)
  var sailboatTransformerStream = sailboatTransformer.setupSailboatTransform(env)

  var boatPartyStream = speedboatTransformerStream.union(sailboatTransformerStream)

  // use keyed state
  //var boatPartyStreamXfm = transformBoatKeyedState(boatPartyStream)

  var boatPartyStreamXfm = transformBoatOperationState(boatPartyStream)

  boatPartyStreamXfm.print()

  val kafkaProducer = new FlinkKafkaProducer[String](
    "localhost:9092",
    "boat_data",
    new SimpleStringSchema)
  boatPartyStreamXfm.addSink(kafkaProducer)

  env.execute()

  def transformBoatKeyedState(stream: DataStream[Boat]): DataStream[String] = {
    stream.keyBy(x => "Boats")
      .map(AreBoatsCollidingWithKeyedState)
      .map(ToJSONString)
  }

  def transformBoatOperationState(stream: DataStream[Boat]): DataStream[String] = {
    stream
      .map(AreBoatsCollidingWithOperationState)
      .map(ToJSONString)
  }
}
