package com.jada.client

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider
import com.amazonaws.regions.Regions
import com.amazonaws.services.sqs.{AmazonSQS, AmazonSQSClientBuilder}
import com.amazonaws.services.sqs.model.SendMessageRequest
import com.azure.storage.blob.BlobServiceClientBuilder
import com.azure.storage.blob.implementation.util.ModelHelper
import com.azure.storage.blob.sas.{
  BlobSasPermission,
  BlobServiceSasSignatureValues
}
import com.microsoft.azure.functions._
import com.microsoft.azure.functions.annotation.{FunctionName, TimerTrigger}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import java.time.{Instant, OffsetDateTime}
import java.util.logging.Logger
import scala.util.Try
import com.amazonaws.regions.Region
import com.microsoft.azure.functions.annotation.HttpTrigger
import com.microsoft.azure.functions.annotation.AuthorizationLevel
import java.util.Optional
// docker compose up -d
// export SF_PASSWORD=
//mvn clean compile exec:java -Dexec.mainClass=com.jada.client.Functions
object Functions extends App {
  val f = new Functions()
  f.runProducePlayers("", null)
  f.spark.stop()
}
class Functions {

  lazy val spark = SparkSession.builder
    .master("local[*]")
    .appName("client")
    .getOrCreate()

  def getenv(key: String, defaultValue: String): String = {
    Option(System.getenv(key)).getOrElse(defaultValue)
  }
  val topic = getenv("TOPIC", "leovegas-players-history")
  val kafkaBrokers = getenv("KAFKA_BROKERS", "10.10.1.14:9092,10.10.1.13:9092,10.10.1.12:9092")
  val sfOptions = Map(
    "sfPassword" -> getenv("SF_PASSWORD", "NOT_HERE"),
    "sfURL" -> getenv("SF_URL", "irzzdfb-bj65063.snowflakecomputing.com"),
    "sfUser" -> getenv("SF_USER", "APP_MIDDLEWARE"),
    "sfDatabase" -> getenv("SF_DATABASE", "CENTRALDW"),
    "sfSchema" -> getenv("SF_SCHEMA", "BUS"),
    "sfWarehouse" -> getenv("SF_WAREHOUSE", "DELIVERY_WH")
  )

  @FunctionName("producePlayers")
  def runProducePlayers(
                         @TimerTrigger(
                           name = "producePlayers",
                           schedule = "0 25 7 * * *"
                         ) timerInfo: String,
                         context: ExecutionContext
                       ): Unit = {
    implicit val logger = Option(context)
      .map(_.getLogger)
      .getOrElse(java.util.logging.Logger.getGlobal())
    logger.info(s"starting app $timerInfo")
    val df = spark.read
      .format(net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME)
      .option(
        "query",
        s"""
           |select brand_id || '-' || player_id as key, '{"player_id":"'||player_id||'","brand_id":"'||brand_id|| '","reg_datetime":"'||reg_datetime||'"}' as value
           |from dbo.dim_players
           |where brand_id = 22
           |""".stripMargin
      )
      .options(sfOptions)
      .load()

    import spark.implicits._
    import functions._
    val newDf = df.select(
      df.col("key").as("key"),
      df.col("value").as("value")
    )
    newDf.show(1)
    newDf.write
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("topic", topic)
      .save()
  }
}
