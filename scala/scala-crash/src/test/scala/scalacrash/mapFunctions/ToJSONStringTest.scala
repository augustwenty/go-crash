package scalacrash.mapFunctions

import net.liftweb.json.JsonAST.compactRender
import net.liftweb.json.JsonDSL._
import org.scalatest.funsuite.AnyFunSuite
import scalacrash.caseClasses.Boat

class ToJSONStringTest extends AnyFunSuite {

  test("output boat json correctly") {
    val boatObj = Boat("SS Hare", "Speedboat", Map("x"->1.0F, "y"->2.0F), Map("x"->1.1F, "y"->2.1F), 1.2F, .111F)
    val boatJSON = ToJSONString.map(boatObj)

    val expectedJSON =
      ("Name" -> boatObj.Name) ~
      ("Type" -> boatObj.Type) ~
      ("Position" ->
        ("x" -> boatObj.Position("x")) ~
        ("y" -> boatObj.Position("y")) ) ~
      ("Velocity" ->
        ("x" -> boatObj.Velocity("x")) ~
        ("y" -> boatObj.Velocity("y")) ) ~
      ("Orientation" -> boatObj.Orientation) ~
      ("Timestamp" -> boatObj.Timestamp) ~
      ("Colliding" -> false)

    assert(boatJSON == compactRender(expectedJSON))
  }
}