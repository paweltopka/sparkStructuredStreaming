name := "sparkStructuredStreaming"

version := "0.1"

scalaVersion := "2.12.8"
val sparkVersion = "2.4.0"

// grading libraries
libraryDependencies += "junit" % "junit" % "4.10" % Test
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.apache.spark" %% "spark-streaming" % sparkVersion // % "provided
)

