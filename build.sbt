name := "acme"

version := "0.1"

scalaVersion := "2.12.10"

fork in run := true

scalacOptions ++= Seq("-deprecation", "-feature")

resolvers += Resolver.bintrayIvyRepo("com.eed3si9n", "sbt-plugins")

libraryDependencies ++= Seq(
  "dev.zio" %% "zio" % "1.0.0-RC17",
  "com.github.pureconfig" %% "pureconfig" % "0.12.1",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.log4s" %% "log4s" % "1.8.2",
)