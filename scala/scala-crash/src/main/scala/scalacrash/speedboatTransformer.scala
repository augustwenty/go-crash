package scalacrash

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import java.util.Properties

object speedboatTransformer extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "localhost:9092")
  properties.setProperty("group.id", "g3")

  val kafkaConsumer = new FlinkKafkaConsumer[String](
                "raw_speedboat_data",
                new SimpleStringSchema,
                properties) 

  val kafkaProducer = new FlinkKafkaProducer[String](
                "localhost:9092",
                "boat_data",
                new SimpleStringSchema)

  val speedboatTransform = transformSpeedboat(env.addSource(kafkaConsumer))

  speedboatTransform.addSink(kafkaProducer)
  speedboatTransform.print()
  env.execute()

  def transformSpeedboat(stream: DataStream[String]) : DataStream[String] = {
            stream.map(Speedboat)
                  .map(x => Boat.transform(x))
                  .map(x => Boat.toJSONString(x))
        }
}