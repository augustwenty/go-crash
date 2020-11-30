package scalacrash.transformers

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time._
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.util.Collector
import scalacrash.caseClasses.Boat
import scalacrash.mapFunctions.{AreBoatsCollidingFlatMap, AreBoatsCollidingWithKeyedState, AreBoatsCollidingWithOperationState, BoatDedupe, ToJSONString}


object BoatTransformer extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.getConfig.setAutoWatermarkInterval(1000) // 1 sec
  //  env.setParallelism(1)

  var speedboatTransformerStream = SpeedboatTransformer.setupSpeedboatTransformer(env)
  var sailboatTransformerStream = SailboatTransformer.setupSailboatTransform(env)

  var boatPartyStreamXfm = boatSynchronousJoinAndProcess(speedboatTransformerStream, sailboatTransformerStream)

  // var boatPartyStream = speedboatTransformerStream.union(sailboatTransformerStream)
  // use keyed state
  // var boatPartyStreamXfm = transformBoatKeyedState(boatPartyStream)
  // use Op state
  // var boatPartyStreamXfm = transformBoatOperationState(boatPartyStream)

  boatPartyStreamXfm.print()

  val kafkaProducer = new FlinkKafkaProducer[String](
    "localhost:9092",
    "boat_data",
    new SimpleStringSchema)
  boatPartyStreamXfm.addSink(kafkaProducer)

  env.execute()

  def boatSynchronousJoinAndProcess(streamA: DataStream[Boat], streamB: DataStream[Boat]): DataStream[String] = {
    val streamATimestamped = streamA.assignAscendingTimestamps(boat => (boat.Timestamp*1000).toLong)
    val streamBTimestamped = streamB.assignAscendingTimestamps(boat => (boat.Timestamp*1000).toLong)

    streamATimestamped
      .keyBy(x => "sync")
      .intervalJoin(streamBTimestamped.keyBy(x => "sync"))
      .between(Time.milliseconds(-5), Time.milliseconds(5))
      .process(new ProcessJoinFunction[Boat, Boat, Boat] {
        override def processElement(left: Boat, right: Boat, ctx: ProcessJoinFunction[Boat, Boat, Boat]#Context, out: Collector[Boat]): Unit = {
          out.collect(left)
          out.collect(right)
        }
      })
      .flatMap(new BoatDedupe)
      .flatMap(new AreBoatsCollidingFlatMap)
      .map(ToJSONString)
  }

  def transformBoatKeyedState(stream: DataStream[Boat]): DataStream[String] = {
    stream.keyBy(x => "Boats")
      .map(new AreBoatsCollidingWithKeyedState)
      .map(ToJSONString)
  }

  def transformBoatOperationState(stream: DataStream[Boat]): DataStream[String] = {
    stream
      .map(AreBoatsCollidingWithOperationState)
      .map(ToJSONString)
  }
}
