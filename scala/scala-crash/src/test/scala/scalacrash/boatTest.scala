package scalacrash

import org.scalatest.funsuite.AnyFunSuite

class BoatUnitTest extends AnyFunSuite {

  test("output boat json correctly") {
    val boatObj = Boat("SS Hare", "Speedboat", Map("x"->1.0F, "y"->2.0F), Map("x"->1.1F, "y"->2.1F), 1.2F, .111F)
    val boatJSON = Boat.toJSONStringBrutish(boatObj)
    val expectedJSON = """{"Name":"SS Hare","Type":"Speedboat","Position":{"x":1.0,"y":2.0},"Velocity":{"x":1.1,"y":2.1},"Orientation":1.2,"Timestamp":0.111}"""
    assert(boatJSON == expectedJSON)
  }
}