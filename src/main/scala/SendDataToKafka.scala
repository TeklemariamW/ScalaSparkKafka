/*
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.net.{HttpURLConnection, URL}
import scala.io.Source

object SendDataToKafka {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("My Spark Application")
      .master("local[*]")
      .getOrCreate()

    while (true) {
      import spark.implicits._
      //val apiUrl = "https://api.tfl.gov.uk/Line/victoria/Arrivals?app_id=92293faa428041caad3dd647d39753a0&app_key=ba72936a3db54b4ba5792dc8f7acc043"
      val apiUrl = "http://3.8.164.165:5000/api/data"
      val url = new URL(apiUrl)
      val connection = url.openConnection().asInstanceOf[HttpURLConnection]
      connection.setRequestMethod("GET")

      val inputStream = connection.getInputStream
      val response = Source.fromInputStream(inputStream).mkString
      inputStream.close()

      val dfFromText = spark.read.json(Seq(response).toDS)

      /*
      // Select the columns you want to include in the message
      val messageDF = dfFromText.select($"id", $"stationName", $"lineName", $"towards",
        $"expectedArrival", $"vehicleId", $"platformName", $"direction", $"destinationName",
        $"timestamp", $"timeToStation", $"currentLocation", $"timeToLive")
      */

      // Assuming dfFromText is your DataFrame containing data from the API
      val messageDF = dfFromText.select(
        $"Bathrooms",
        $"Bedrooms",
        $"Neighborhood",
        $"Price",
        $"SquareFeet",
        $"YearBuilt"
      )


      val kafkaServer: String = "ip-172-31-3-80.eu-west-2.compute.internal:9092"
      val topicSampleName: String = "arrivaldata"

      messageDF.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaServer)
        .option("topic", topicSampleName)
        .save()

      println("Message is loaded to Kafka topic")
      Thread.sleep(10000) // wait for 10 seconds before making the next call
    }
  }

}
 */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.net.{HttpURLConnection, URL}
import scala.io.Source

object SendDataToKafka {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("My Spark Application")
      .master("local[*]")
      .getOrCreate()

    while (true) {
      try {
        import spark.implicits._

        val apiUrl = "http://3.8.164.165:5000/api/data"
        val url = new URL(apiUrl)
        val connection = url.openConnection().asInstanceOf[HttpURLConnection]
        connection.setRequestMethod("GET")

        val inputStream = connection.getInputStream
        val response = Source.fromInputStream(inputStream).mkString
        inputStream.close()

        val dfFromText = spark.read.json(Seq(response).toDS)

        // Selecting the relevant columns from the dataset
        val messageDF = dfFromText.select(
          $"Bathrooms",
          $"Bedrooms",
          $"Neighborhood",
          $"Price",
          $"SquareFeet",
          $"YearBuilt"
        )

        // Create a unique key for Kafka messages, e.g., concatenating Neighborhood and YearBuilt
        val messageWithKeyDF = messageDF.withColumn("key", concat($"Neighborhood", lit("_"), $"YearBuilt"))

        val kafkaServer: String = "ip-172-31-3-80.eu-west-2.compute.internal:9092"
        val topicSampleName: String = "arrivaldata"

        messageWithKeyDF.selectExpr("CAST(key AS STRING)", "to_json(struct(*)) AS value")
          .write
          .format("kafka")
          .option("kafka.bootstrap.servers", kafkaServer)
          .option("topic", topicSampleName)
          .save()

        println("Message is loaded to Kafka topic")
      } catch {
        case e: Exception => println(s"Error occurred: ${e.getMessage}")
      }

      Thread.sleep(10000) // wait for 10 seconds before making the next call
    }
  }
}
