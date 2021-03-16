name := "Lab 1"
version := "1.0"
scalaVersion := "2.12.12"

scalastyleFailOnWarning := true

val sparkVersion = "2.4.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
