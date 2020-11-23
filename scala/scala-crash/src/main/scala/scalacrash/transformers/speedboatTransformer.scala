package scalacrash.transformers

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import scalacrash.caseClasses.Boat
import scalacrash.mapFunctions.{CalculateSpeedboatOrientation, ToSpeedboat}

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

  def transformSpeedboat(stream: DataStream[String]): DataStream[Boat] = {
    stream.map(ToSpeedboat)
      .map(CalculateSpeedboatOrientation)
  }
}
