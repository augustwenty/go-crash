
scalaVersion := "2.12.12"
name := "scala-crash"
organization := "ch.epfl.scala"
version := "1.0"


libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
libraryDependencies += "org.apache.flink" %% "flink-streaming-scala" % "1.11.2"
libraryDependencies += "org.apache.flink" %% "flink-clients" % "1.11.2"
libraryDependencies += "org.apache.flink" %% "flink-connector-kafka" % "1.11.2"
libraryDependencies += "org.apache.flink" %% "flink-test-utils" % "1.11.2" % Test
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % Test
libraryDependencies += "org.scalatest" %% "scalatest-funsuite" % "3.2.2" % Test
libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"
libraryDependencies += "net.liftweb" %% "lift-json" % "3.4.2"