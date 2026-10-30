// revenue-in-flight: an event pipeline you see in money.

ThisBuild / organization := "io.github.provenmap"
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.9.0"
ThisBuild / scalacOptions ++= Seq("-deprecation", "-feature", "-Wunused:imports")

val kafkaVersion = "4.4.0"
val ujson        = "com.lihaoyi"   %% "ujson"        % "4.4.3"
val munit        = "org.scalameta" %% "munit"        % "1.3.6" % Test
val slf4jSimple  = "org.slf4j"      % "slf4j-simple" % "2.0.17"
val kafka        = "org.apache.kafka" % "kafka-clients" % kafkaVersion

/** Tests run from the repository root so they can read the catalog. */
lazy val common = Seq(
  Test / fork := true,
  Test / baseDirectory := (ThisBuild / baseDirectory).value,
  run / fork := true,
  run / baseDirectory := (ThisBuild / baseDirectory).value
)

lazy val root = (project in file("."))
  .aggregate(catalog, simulator, observer)
  .settings(name := "revenue-in-flight", publish / skip := true)

/** The event catalog and CloudEvents helpers. No Kafka. */
lazy val catalog = (project in file("catalog"))
  .settings(common, name := "rif-catalog", libraryDependencies ++= Seq(ujson, munit),
    // The catalog lives beside the module and ships on the classpath.
    Compile / unmanagedResources += baseDirectory.value / "order-to-cash.json")

/** Order traffic and the payments, inventory and shipping services, with a control API. */
lazy val simulator = (project in file("simulator"))
  .dependsOn(catalog)
  .settings(common, name := "rif-simulator", libraryDependencies ++= Seq(kafka, slf4jSimple, munit))

/** Prices every event, tracks money at risk, streams it over WebSocket, serves the view. */
lazy val observer = (project in file("observer"))
  .dependsOn(catalog)
  .settings(common, name := "rif-observer",
    libraryDependencies ++= Seq(kafka, slf4jSimple, "io.javalin" % "javalin" % "6.7.0", munit))
