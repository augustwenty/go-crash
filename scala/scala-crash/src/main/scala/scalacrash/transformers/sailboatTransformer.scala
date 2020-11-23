package scalacrash.transformers

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import scalacrash.caseClasses.{Boat, Sailboat}
import scalacrash.mapFunctions.{CalculateSailboatOrientationWithKeyedState, ToSailboat}

object sailboatTransformer {

  def setupSailboatTransform(env: StreamExecutionEnvironment): DataStream[Boat] = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "g3")

    val kafkaConsumer = new FlinkKafkaConsumer[String](
      "raw_sailboat_data",
      new SimpleStringSchema,
      properties)

    transformSailboatCountWindow(env.addSource(kafkaConsumer))
  }

  def transformSailboatRichMap(stream: DataStream[String]): DataStream[Boat] = {
    stream.map(ToSailboat)
      .keyBy(x => x.Name)
      .map(CalculateSailboatOrientationWithKeyedState)
      .filter(x => x.isDefined)
      .map(x => x.get)
  }

  def transformSailboatCountWindow(stream: DataStream[String]): DataStream[Boat] = {
    stream.map(ToSailboat)
      .keyBy(x => x.Name)
      .countWindow(2, 1)
      .process(new MyProcessWindowFunction)
  }

  class MyProcessWindowFunction extends ProcessWindowFunction[Sailboat, Boat, String, GlobalWindow] {

    def process(key: String, context: Context, input: Iterable[Sailboat], out: Collector[Boat]): Unit = {
      if (input.size != 2) return


      val r0: Sailboat = input.head
      val r1: Sailboat = input.last // Most recent
      val vx: Float = (r1.Position("x") - r0.Position("x")) / (r1.Timestamp - r0.Timestamp)
      val vy: Float = (r1.Position("y") - r0.Position("y")) / (r1.Timestamp - r0.Timestamp)
      val velocity = Map("x" -> vx, "y" -> vy)
      val orientation = Math.atan2(vy, vx).toFloat

      if (!vx.isNaN && !vy.isNaN) {
        out.collect(Boat(r0.Name, "Sailboat", r0.Position, velocity, orientation, r0.Timestamp))
      }

    }
  }

}
