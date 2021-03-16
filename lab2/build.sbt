name := "Lab2"

version := "1.0"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.7"

resolvers ++= Seq(
  "osgeo" at "https://repo.osgeo.org/repository/release",
  "confluent" at "https://packages.confluent.io/maven"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  //"org.apache.logging.log4j" %% "log4j-api-scala" % "11.0",
  "org.locationtech.geomesa" %% "geomesa-spark-jts" % "2.4.1",
  //"org.locationtech.geomesa" %% "geomesa-utils" % "2.4.1",
  "com.uber" % "h3" % "3.0.3"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}