package scalacrash.transformers

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, ProcessAllWindowFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time._
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.util.Collector
import scalacrash.caseClasses.Boat
import scalacrash.flatMapFunctions.{AreBoatsCollidingFlatMap, BoatDedupe}
import scalacrash.mapFunctions.{AreBoatsCollidingWithKeyedState, AreBoatsCollidingWithOperationState, ToJSONString}


object BoatTransformer extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.getConfig.setAutoWatermarkInterval(200) // 200 ms
  //  env.setParallelism(1)

  var speedboatTransformerStream = SpeedboatTransformer.setupSpeedboatTransformer(env)
  var sailboatTransformerStream = SailboatTransformer.setupSailboatTransform(env)

  var boatPartyStreamXfm = boatSynchronousJoinAndProcess(speedboatTransformerStream, sailboatTransformerStream)
//  var boatPartyStreamXfm = boatSynchronousJoinAndWindowProcess(speedboatTransformerStream, sailboatTransformerStream)

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
//      .filter(boat => boat.Type == "speedboat" || boat.Timestamp < 5.0)
      .flatMap(new AreBoatsCollidingFlatMap(.001F, .5F))
      .map(ToJSONString)
  }

  def boatSynchronousJoinAndWindowProcess(streamA: DataStream[Boat], streamB: DataStream[Boat]): DataStream[String] = {
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
//      .map(x => {
//        println(x)
//        x
//      })
//      .keyBy(x => "sync")
      .assignAscendingTimestamps(boat => (1000*boat.Timestamp).toLong)
      .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(200)))
      .process(new CollisionDetectorUsingAllWindow)
//      .process(new CollisionDetectorUsingWindow)
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

class CollisionDetectorUsingWindow extends ProcessWindowFunction[Boat, Boat, String, TimeWindow] {

   def process(key: String, context: Context, input: Iterable[Boat], out: Collector[Boat]): Unit = {
    println(s"window size: ${input.size}")
    out.collect(input.head)
  }
}

class CollisionDetectorUsingAllWindow extends ProcessAllWindowFunction[Boat, Boat, TimeWindow] {

  override def process(context: Context, elements: Iterable[Boat], out: Collector[Boat]): Unit = {
    println(s"window size: ${elements.size}")
    out.collect(elements.head)
  }
}

