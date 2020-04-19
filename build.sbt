
name := "AggregationApp"

version := "0.1"

scalaVersion := "2.13.1"

val AkkaVersion = "2.6.4"
val AlpakkaVersion = "2.0.0-RC2"
val AlpakkaKafkaVersion = "2.0.2"

libraryDependencies ++= Seq(
  // #deps
  "com.typesafe.akka"     %%  "akka-stream-kafka"               % AlpakkaKafkaVersion,
  "com.typesafe.akka"     %%  "akka-stream"                     % AkkaVersion,
  "com.typesafe.akka"     %%  "akka-actor-typed"                % AkkaVersion,
  "com.typesafe.akka"     %%  "akka-actor"                      % AkkaVersion,
  "com.lightbend.akka"    %%  "akka-stream-alpakka-cassandra"   % AlpakkaVersion,
  // Logging
  "com.typesafe.akka"     %% "akka-slf4j"                       % AkkaVersion,
  "ch.qos.logback"        % "logback-classic"                   % "1.2.3",
  // #deps
  "com.thesamet.scalapb"  %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"
)

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)