name := "acme"

version := "0.1"

scalaVersion := "2.12.10"

fork in run := true

scalacOptions ++= Seq("-deprecation", "-feature")
scalacOptions += "-Ylog-classpath"
resolvers += Resolver.bintrayIvyRepo("com.eed3si9n", "sbt-plugins")

libraryDependencies ++= Seq(
  "com.github.pureconfig" %% "pureconfig" % "0.12.1",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.apache.spark" %% "spark-sql" % "2.4.5"  % "provided"
)

libraryDependencies ~= { _.map(_.exclude("org.slf4j", "slf4j-log4j12"))}
publishTo := Some(Resolver.file("file", new File("C:\\Users\\Indra\\Documents\\ingestion")))
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) =>
    xs map {_.toLowerCase} match {
      case "manifest.mf" :: Nil | "index.list" :: Nil | "dependencies" :: Nil =>
        MergeStrategy.discard
      case ps @ x :: xs if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "plexus" :: xs =>
        MergeStrategy.discard
      case "services" :: xs =>
        MergeStrategy.filterDistinctLines
      case "spring.schemas" :: Nil | "spring.handlers" :: Nil =>
        MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.first
    }
  case "application.conf" => MergeStrategy.concat
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}