/*//package kafkaspark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object ReadFromKafka {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("KafkaToJson").master("local[*]").getOrCreate()

    // Define the Kafka parameters
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "ip-172-31-3-80.eu-west-2.compute.internal:9092",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "group1",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // Define the Kafka topic to subscribe to
    val topic = "arrivaldata"

    // Define the schema for the JSON messages
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = true),
      StructField("stationName", StringType, nullable = true),
      StructField("lineName", StringType, nullable = true),
      StructField("towards", StringType, nullable = true),
      StructField("expectedArrival", StringType, nullable = true),
      StructField("vehicleId", StringType, nullable = true),
      StructField("platformName", StringType, nullable = true),
      StructField("direction", StringType, nullable = true),
      StructField("destinationName", StringType, nullable = true),
      StructField("timestamp", StringType, nullable = true),
      StructField("timeToStation", StringType, nullable = true),
      StructField("currentLocation", StringType, nullable = true),
      StructField("timeToLive", StringType, nullable = true)
    ))

    // Read the JSON messages from Kafka as a DataFrame
    val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "ip-172-31-3-80.eu-west-2.compute.internal:9092").option("subscribe", topic).option("startingOffsets", "earliest").load().select(from_json(col("value").cast("string"), schema).as("data")).selectExpr("data.*")

    // Write the DataFrame as CSV files to HDFS
    df.writeStream.format("csv").option("checkpointLocation", "/tmp/jenkins/kafka/trainarrival/checkpoint").option("path", "/tmp/jenkins/kafka/trainarrival/data").start().awaitTermination()

  }

}*/
//package kafkaspark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ReadFromKafka {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaToJson")
      .master("local[*]")
      .getOrCreate()

    // Define the Kafka parameters
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "ip-172-31-3-80.eu-west-2.compute.internal:9092",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "group1",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // Define the Kafka topic to subscribe to
    val topic = "arrivaldata" // Change the topic to your new topic

    // Define the schema for the JSON messages
    val schema = StructType(Seq(
      StructField("Bathrooms", IntegerType, nullable = true),
      StructField("Bedrooms", IntegerType, nullable = true),
      StructField("Neighborhood", StringType, nullable = true),
      StructField("Price", DoubleType, nullable = true),
      StructField("SquareFeet", IntegerType, nullable = true),
      StructField("YearBuilt", IntegerType, nullable = true)
    ))

    // Read the JSON messages from Kafka as a DataFrame
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "ip-172-31-3-80.eu-west-2.compute.internal:9092")
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()
      .select(from_json(col("value").cast("string"), schema).as("data"))
      .selectExpr("data.*")

    // Write the DataFrame as CSV files to HDFS (or any other storage)
    df.writeStream
      .format("csv")
      .option("checkpointLocation", "/tmp/jenkins/kafka/houseprice/checkpoint")
      .option("path", "/tmp/jenkins/kafka/houseprice/data")
      .start()
      .awaitTermination()
  }
}
