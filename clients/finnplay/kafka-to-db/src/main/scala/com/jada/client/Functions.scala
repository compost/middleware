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
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.streaming.Trigger
// docker compose up -d
// export SF_PASSWORD=
//mvn clean compile exec:java -Dexec.mainClass=com.jada.client.Functions
object Functions extends App {
  val f = new Functions()
  f.insertPlayers("", null)
  f.spark.stop()
}
class Functions {

  lazy val spark = SparkSession.builder
    .master("local[*]")
    .appName("sparkounais")
    .getOrCreate()

  def getenv(key: String, defaultValue: String): String = {
    Option(System.getenv(key)).getOrElse(defaultValue)
  }

  val kafkaBrokers = getenv("KAFKA_BROKERS", "127.0.0.1:9092")
  val sfOptions = Map(
    "sfPassword" -> getenv("SF_PASSWORD", "NOT_HERE"),
    "sfURL" -> getenv("SF_URL", "irzzdfb-bj65063.snowflakecomputing.com"),
    "sfUser" -> getenv("SF_USER", "APP_MIDDLEWARE"),
    "sfDatabase" -> getenv("SF_DATABASE", "CENTRALDW"),
    "sfSchema" -> getenv("SF_SCHEMA", "bus"),
    "sfWarehouse" -> getenv("SF_WAREHOUSE", "DELIVERY_WH")
  )

  @FunctionName("players")
  def insertPlayers(
      @TimerTrigger(
        name = "Players",
        schedule = "0 25 7 * * *"
      ) timerInfo: String,
      context: ExecutionContext
  ): Unit = {
    implicit val logger = Option(context)
      .map(_.getLogger)
      .getOrElse(java.util.logging.Logger.getGlobal())
    logger.info(s"starting app $timerInfo")

    val schema: StructType = StructType(
      Seq(
        StructField("player_id", StringType),
        StructField("playerId", StringType),
        StructField("originalId", StringType),
        StructField("brand_id", StringType),
        StructField("reg_datetime", StringType),
        StructField("first_name", StringType),
        StructField("last_name", StringType),
        StructField("email", StringType),
        StructField("phone_number", StringType),
        StructField("language", StringType),
        StructField("affiliate_id", StringType),
        StructField("is_self_excluded", StringType),
        StructField("first_dep_datetime", StringType),
        StructField("dob", StringType),
        StructField("country_id", StringType),
        StructField("vip", StringType),
        StructField("test_user", StringType),
        StructField("currency_id", StringType),
        StructField("last_login_datetime", StringType),
        StructField("nick_name", StringType),
        StructField("gender_id", StringType),
        StructField("gender_description", StringType),
        StructField("address", StringType),
        StructField("zip_code", StringType),
        StructField("city", StringType),
        StructField("receive_email", StringType),
        StructField("device_type_id", StringType),
        StructField("device_type_description", StringType),
        StructField("last_deposit_datetime", StringType),
        StructField("player_status_id", StringType),
        StructField("player_status_description", StringType),
        StructField("user_validated", StringType),
        StructField("user_validated_time", StringType)
      )
    )
    val df = spark.read.schema(schema).json("/tmp/all.json")

    import spark.implicits._
    import functions._

    df.write
      .format(net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME)
      .options(sfOptions)
      .option("dbtable", "MIDDLEWARE_PLAYNORTH_HISTORY")
      .mode(SaveMode.Append)
      .save()

  }
  @FunctionName("players")
  def runPlayers(
      @TimerTrigger(
        name = "Players",
        schedule = "0 25 7 * * *"
      ) timerInfo: String,
      context: ExecutionContext
  ): Unit = {
    val storageName = "centraliseddwa6a1"
    val storageAccessKey = System.getenv("STORAGE_ACCESS_KEY")
    val outputContainerName = "playnorthtmp"
    spark.conf.set(
      s"fs.azure.account.key.$storageName.blob.core.windows.net",
      storageAccessKey
    )
    val configObj = new Configuration()
    configObj.set(
      "fs.defaultFS",
      s"wasbs://$outputContainerName@$storageName.blob.core.windows.net"
    )
    configObj.set(
      "fs.azure.account.key." + storageName + ".blob.core.windows.net",
      storageAccessKey
    )
    val fs = Try(FileSystem.newInstance(configObj))
      .getOrElse(throw new Exception(s"Cannot access storage"))
    val outputContainerPath =
      s"wasbs://$outputContainerName@$storageName.blob.core.windows.net"
    val folder = "data-in-json"

    implicit val logger = Option(context)
      .map(_.getLogger)
      .getOrElse(java.util.logging.Logger.getGlobal())
    logger.info(s"starting app $timerInfo")

    val schema: StructType = StructType(
      Seq(
        StructField("player_id", StringType),
        StructField("playerId", StringType),
        StructField("originalId", StringType),
        StructField("brand_id", StringType),
        StructField("reg_datetime", StringType),
        StructField("first_name", StringType),
        StructField("last_name", StringType),
        StructField("email", StringType),
        StructField("phone_number", StringType),
        StructField("language", StringType),
        StructField("affiliate_id", StringType),
        StructField("is_self_excluded", StringType),
        StructField("first_dep_datetime", StringType),
        StructField("dob", StringType),
        StructField("country_id", StringType),
        StructField("vip", StringType),
        StructField("test_user", StringType),
        StructField("currency_id", StringType),
        StructField("last_login_datetime", StringType),
        StructField("nick_name", StringType),
        StructField("gender_id", StringType),
        StructField("gender_description", StringType),
        StructField("address", StringType),
        StructField("zip_code", StringType),
        StructField("city", StringType),
        StructField("receive_email", StringType),
        StructField("device_type_id", StringType),
        StructField("device_type_description", StringType),
        StructField("last_deposit_datetime", StringType),
        StructField("player_status_id", StringType),
        StructField("player_status_description", StringType),
        StructField("user_validated", StringType),
        StructField("user_validated_time", StringType)
      )
    )
    val df = spark.readStream
      .format("kafka")
      .option("maxOffsetsPerTrigger", 500000)
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("startingOffsets", "earliest")
      .option("subscribe", "finnplay-processor-v3-players-store-changelog")
      .load()

    import spark.implicits._
    import functions._

    import org.apache.hadoop.fs._

    val folderExtension = "players"
    val outputBlobFolder = s"$outputContainerPath/$folder/$folderExtension"
    val v =
      df.select(from_json(col("value").cast("string"), schema).alias("value"))
    v.printSchema()
    val e = v.selectExpr("value.*")
    e.printSchema()
    e.writeStream
      .format("json")
      .option("checkpointLocation", "/tmp/checkpoint/")
      .start(outputBlobFolder)
      .awaitTermination()

  }
}
