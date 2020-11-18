package scalacrash

import java.util.Properties

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.util.Collector

object sailboatTransformer extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "localhost:9092")
  properties.setProperty("group.id", "g3")

  val kafkaConsumer = new FlinkKafkaConsumer[String](
    "raw_sailboat_data",
    new SimpleStringSchema,
    properties)

  val kafkaProducer = new FlinkKafkaProducer[String](
    "localhost:9092",
    "boat_data",
    new SimpleStringSchema)

  val sailboatTransformer = transformSailboat(env.addSource(kafkaConsumer))

  sailboatTransformer.addSink(kafkaProducer)
  sailboatTransformer.print()
  env.execute()

  class MyProcessWindowFunction extends ProcessWindowFunction[Sailboat, Boat, String, GlobalWindow ] {
    
    def process(key: String, context:Context, input: Iterable[Sailboat], out: Collector[Boat]) = {
      val r0: Sailboat = input.head
      val r1: Sailboat = input.last // Most recent
      val vx: Float = (r1.Position("x") - r0.Position("x"))/(r1.Timestamp-r0.Timestamp)
      val vy: Float = (r1.Position("y") - r0.Position("y"))/(r1.Timestamp-r0.Timestamp)
      val velocity = Map("x"->vx, "y"->vy)
      val orientation = Math.atan2(vy, vx).toFloat
      out.collect(Boat(r0.Name, "Sailboat", r0.Position, velocity, orientation, r0.Timestamp))
    }
  }


  def transformSailboat(stream: DataStream[String]) : DataStream[String] = {
    stream.map(Sailboat)
      .keyBy(x => x.Name)
      .countWindow(2, 1)
      .process(new MyProcessWindowFunction)
      .map(x => Boat.toJSONString(x))
  }
}
