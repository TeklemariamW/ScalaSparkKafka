name := "SendDataToKafka"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.7",
  "org.apache.spark" %% "spark-sql" % "2.4.7",
  "com.lihaoyi" %% "requests" % "0.6.9"
)

// For Kafka integration, you might also need
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.7"
