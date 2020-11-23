package scalacrash

import org.apache.flink.streaming.api.scala._
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import scalacrash.caseClasses.Speedboat
import scalacrash.mapFunctions.ToSpeedboat

import scala.collection.mutable.ArrayBuffer

class speedboatTransformerTest extends AnyFunSuite with BeforeAndAfter {
  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  before {
    flinkCluster.before()
  }

  after {
    flinkCluster.after()
  }

  test("executes flow") {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    CollectSink.values.clear()

    val speedboatJSON = "{\"Name\":\"Tow Me\",\"Position\":{\"x\":0.7,\"y\":0.5},\"Velocity\":{\"x\":1.0,\"y\":2.0},\"Timestamp\":0.4}"
    env.fromElements[String](speedboatJSON)
      .map(ToSpeedboat)
      .addSink(new CollectSink())

    env.execute()

    print(CollectSink.values)

    val expectedSpeedboat = Speedboat("Tow Me", Map("x" -> 0.7F, "y" -> 0.5F), Map("x" -> 1.0F, "y" -> 2.0F), 0.4F)
    assert(CollectSink.values.head.equals(expectedSpeedboat))
  }

}

class CollectSink extends SinkFunction[Speedboat] {
  override def invoke(value: Speedboat): Unit = {
    synchronized {
      CollectSink.values += value
    }
  }
}

object CollectSink {
  val values = ArrayBuffer[Speedboat]()
}
