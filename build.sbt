name := "akka-essentials"

version := "0.1"

scalaVersion := "2.12.7"

val akkaVersion = "2.5.13"

libraryDependencies ++= Seq(
  // Akka Core and TestKit
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion,

  // Akka Streams
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,

  // ScalaTest for Testing
  "org.scalatest" %% "scalatest" % "3.0.5",

  // Redis (Jedis)
  "redis.clients" % "jedis" % "4.4.3",

  // Kafka Client
  "org.apache.kafka" % "kafka-clients" % "3.6.0",

  // JSON (Optional for better JSON handling)
  "com.typesafe.play" %% "play-json" % "2.6.10"
)

// SLF4J Simple Logger (for simple console output)
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.36"
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.4"
libraryDependencies += "com.google.flatbuffers" % "flatbuffers-java" % "2.0.0"