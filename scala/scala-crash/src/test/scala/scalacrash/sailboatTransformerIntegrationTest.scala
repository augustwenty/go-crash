package scalacrash

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.ArrayBuffer 

class sailboatTransformerIntegrationTest extends AnyFunSuite with BeforeAndAfter{
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

    val sailboatJSON1 = "{\"Name\":\"Tow Me\",\"Position\":{\"x\":0.7,\"y\":0.5},\"Timestamp\":0.4}"
    val sailboatJSON2 = "{\"Name\":\"Tow Me\",\"Position\":{\"x\":0.8,\"y\":0.6},\"Timestamp\":0.5}"
    val sailboatJSON3 = "{\"Name\":\"Tow Me\",\"Position\":{\"x\":0.9,\"y\":0.7},\"Timestamp\":0.6}"

    implicit val typeInfo = TypeInformation.of(classOf[String]) 
    
    val stream = env.fromElements(sailboatJSON1, sailboatJSON2, sailboatJSON3)
    sailboatTransformer.transformSailboat(stream).addSink(new CollectSailboatTransformSink())

    env.execute()

    print(CollectSailboatTransformSink.values)

    val expectedSpeedboat = "{\"Name\":\"Tow Me\",\"Type\":\"speedboat\",\"Position\":{\"x\":0.699999988079071,\"y\":0.5},\"Velocity\":{\"x\":1.0,\"y\":2.0},\"Orientation\":1.1071487665176392,\"Timestamp\":0.4000000059604645}"
    assert(CollectSailboatTransformSink.values.head.equals(expectedSpeedboat))
  }
}

class CollectSailboatTransformSink extends SinkFunction[String] {
  override def invoke(value: String): Unit = {
    synchronized {
      CollectSailboatTransformSink.values += value
    }
  }
}

object CollectSailboatTransformSink {
  val values = ArrayBuffer[String]()
}
