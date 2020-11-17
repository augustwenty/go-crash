package scalacrash

import java.util.Properties

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

object sailboatTransformer {
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

  class MyProcessWindowFunction extends ProcessWindowFunction[(Sailboat, Sailboat), Boat, String, CountWindow ]


  def transformSailboat(stream: DataStream[String]) : DataStream[String] = {
    stream.map(Sailboat)
      .keyBy(x => x.Name)
      .countWindow(2, 1)

      .map(x => Boat.transform(x))
      .map(x => Boat.toJSONString(x))
  }
}
