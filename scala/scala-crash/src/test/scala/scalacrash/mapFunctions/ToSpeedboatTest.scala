package scalacrash.mapFunctions

import org.scalatest.funsuite.AnyFunSuite
import scalacrash.caseClasses.Speedboat

class ToSpeedboatTest extends AnyFunSuite {

  test("parse speedboat json correctly") {
    val speedboatJSON = "{\"Name\":\"Tow Me\",\"Position\":{\"x\":0.7,\"y\":0.5},\"Velocity\":{\"x\":1.0,\"y\":2.0},\"Timestamp\":0.4}"
    val speedboat : Speedboat = ToSpeedboat.map(speedboatJSON)
    assert(speedboat.Name == "Tow Me")
    assert(speedboat.Position("x") > 0.699)
  }
}