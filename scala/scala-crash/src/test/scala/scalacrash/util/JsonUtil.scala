package scalacrash.util

import net.liftweb.json._
import scalacrash.Boat

object JsonUtil {
    def boatToJson(boatJson: String) : Boat = {
        implicit val formats = DefaultFormats
        val jsonObj = parse(boatJson)
        jsonObj.extract[Boat]
    }
}