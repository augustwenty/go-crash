package scalacrash.integration

import net.liftweb.json.{DefaultFormats, parse}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import scalacrash.caseClasses.Boat
import scalacrash.transformers.SpeedboatTransformer

import scala.collection.mutable.ArrayBuffer 

class speedboatTransformerIntegrationTest extends AnyFunSuite with BeforeAndAfter{
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

  def boatToJson(boatJson: String) : Boat = {
    implicit val formats = DefaultFormats
    val jsonObj = parse(boatJson)
    jsonObj.extract[Boat]
  }


  test("executes flow") {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    CollectSpeedboatTransformSink.values.clear()

    val speedboatJSON = "{\"Name\":\"Tow Me\",\"Position\":{\"x\":0.7,\"y\":0.5},\"Velocity\":{\"x\":1.0,\"y\":2.0},\"Timestamp\":0.4}"
    
    val stream = env.fromElements(speedboatJSON)
    SpeedboatTransformer.transformSpeedboat(stream).addSink(new CollectSpeedboatTransformSink())

    env.execute()

    val expectedSpeedboat = boatToJson("{\"Name\":\"Tow Me\",\"Type\":\"speedboat\",\"Position\":{\"x\":0.699999988079071,\"y\":0.5},\"Velocity\":{\"x\":1.0,\"y\":2.0},\"Orientation\":1.1071487665176392,\"Timestamp\":0.4000000059604645,\"Colliding\":false}")
    assert(CollectSpeedboatTransformSink.values.head.equals(expectedSpeedboat))
  }
}

class CollectSpeedboatTransformSink extends SinkFunction[Boat] {
  override def invoke(value: Boat): Unit = {
    synchronized {
      CollectSpeedboatTransformSink.values += value
    }
  }
}

object CollectSpeedboatTransformSink {
  val values = ArrayBuffer[Boat]()
}
