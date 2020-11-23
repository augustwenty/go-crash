package scalacrash

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import java.util.Properties

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration

object speedboatTransformer {

  def setupSpeedboatTransformer(env: StreamExecutionEnvironment): DataStream[Boat] = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "g3")

    val kafkaConsumer = new FlinkKafkaConsumer[String](
      "raw_speedboat_data",
      new SimpleStringSchema,
      properties)

    transformSpeedboat(env.addSource(kafkaConsumer))
  }

  def transformSpeedboat(stream: DataStream[String]) : DataStream[Boat] = {
            stream.map(Speedboat)
                  .map(x => Boat.transform(x))
  }
}