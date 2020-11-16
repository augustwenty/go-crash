package scalacrash

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.common.serialization.SimpleStringSchema
import java.util.Properties
import spray.json._

object speedboatTransformer extends App {
  println("Hello, World!")
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

  def transformSpeedboat(stream: DataStream[String]) : DataStream[String] = {
            // Count how many times a word has been read off of the tatanka Kafka topic in 15 seconds,
            // then send that to kafka as a Tuple to the monkey topic
            stream.flatMap()

            // stream.filter(_ != "poop") // Remove all the poop from the topic
            //     .map { (_, 1) }                       // Convert each element to a Tuple. The original string is the first element, second element is number 1
            //     .keyBy(_._1)                          // Key the stream by the first element in the Tuple
            //     // .timeWindow(Time.seconds(15))         // Batch the steam into 15 second segments
            //     .sum(1)                               // Sum the second element in the Tuple with the second element from the previous Tuple
            //     .map (_.toString())                   // Convert the Tuple to a string for Kafka
        }
  
  def getSpeedboatsFromStream(boatJson: String): 
}

