package scalacrash

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.ArrayBuffer 

class SpeedboatTransformerIntegrationTest extends AnyFunSuite with BeforeAndAfter{
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

    CollectSpeedboatTransformSink.values.clear()

    val speedboatJSON = "{\"Name\":\"Tow Me\",\"Position\":{\"x\":0.7,\"y\":0.5},\"Velocity\":{\"x\":1.0,\"y\":2.0},\"Timestamp\":0.4}"
    
    implicit val typeInfo = TypeInformation.of(classOf[String]) 
    
    val stream = env.fromElements(speedboatJSON)
    SpeedboatTransformer.transformSpeedboat(stream).addSink(new CollectSpeedboatTransformSink())

    env.execute()

    val expectedSpeedboat = "{\"Name\":\"Tow Me\",\"Type\":\"speedboat\",\"Position\":{\"x\":0.699999988079071,\"y\":0.5},\"Velocity\":{\"x\":1.0,\"y\":2.0},\"Orientation\":1.1071487665176392,\"Timestamp\":0.4000000059604645}"
    assert(CollectSpeedboatTransformSink.values.head.equals(expectedSpeedboat))
  }
}

class CollectSpeedboatTransformSink extends SinkFunction[String] {
  override def invoke(value: String): Unit = {
    synchronized {
      CollectSpeedboatTransformSink.values += value
    }
  }
}

object CollectSpeedboatTransformSink {
  val values = ArrayBuffer[String]()
}
