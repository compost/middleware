package com.jada.finnplay

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

import com.microsoft.azure.functions.annotation.HttpTrigger
import java.time.{Instant, OffsetDateTime}
import java.util.logging.Logger
import scala.util.Try
import com.microsoft.azure.functions.annotation.AuthorizationLevel
import java.util.Optional

import java.security.PrivateKey
import java.util.Base64
import java.security.Security
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter
import org.bouncycastle.openssl.PEMParser
import java.io.StringReader
class Functions {

  def getPrivateKey: PrivateKey = {
    Security.addProvider(new BouncyCastleProvider())
    // Read an object from the private key file.
    val base64 = System.getenv("DATASOURCE_PEM_KEY_BASE64")
    val passphrase = System.getenv("DATASOURCE_PASSPHRASE")
    val value = new String(Base64.getDecoder().decode(base64))
    val pemParser = new PEMParser(new StringReader(value))
    val pemObject = pemParser.readObject()
    // Handle the case where the private key is encrypted.
    val encryptedPrivateKeyInfo =
      pemObject.asInstanceOf[PKCS8EncryptedPrivateKeyInfo]
    val pkcs8Prov =
      new JceOpenSSLPKCS8DecryptorProviderBuilder()
        .build(passphrase.toCharArray());
    val privateKeyInfo =
      encryptedPrivateKeyInfo.decryptPrivateKeyInfo(pkcs8Prov)
    pemParser.close();
    val converter =
      new JcaPEMKeyConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME);
    converter.getPrivateKey(privateKeyInfo);
  }

  def getPemPrivateKey(): String = {
    val encodedPrivateKey = getPrivateKey.getEncoded();
    val base64EncodedPrivateKey =
      Base64.getEncoder().encodeToString(encodedPrivateKey);

    val pemBuilder = new StringBuilder();
    pemBuilder.append(base64EncodedPrivateKey);

    return pemBuilder.toString();
  }
  val Deposit = 22

  val playnorth100 = new Brand(
    id = 100,
    folder = "playnorth-100",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/play-north-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val playnorth101 = new Brand(
    id = 101,
    folder = "playnorth-101",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/play-north-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val vegasonline = new Brand(
    id = 106,
    folder = "vegasonline",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/vegasonline-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )

  lazy val spark = SparkSession.builder
    .master("local[*]")
    .appName("finnplayGetBatchUpdate")
    .config("spark.driver.memory", "1G")
    .getOrCreate()

  val dryRun = Option(System.getenv("DRY_RUN")).map(_.toBoolean).getOrElse(true)
  val storageName = "centraliseddwa6a1"
  val storageAccessKey = System.getenv("STORAGE_ACCESS_KEY")
  val outputContainerName = "finnplay"
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
  val folder = "data"

  def getenv(key: String, defaultValue: String): String = {
    Option(System.getenv(key)).getOrElse(defaultValue)
  }

  val sfOptions = Map(
    "pem_private_key" -> getPemPrivateKey(),
    "sfURL" -> getenv("SF_URL", "irzzdfb-bj65063.snowflakecomputing.com"),
    "sfUser" -> getenv("SF_USER", "APP_MIDDLEWARE_SERVICE"),
    "sfDatabase" -> getenv("SF_DATABASE", "CENTRALDW"),
    "sfSchema" -> getenv("SF_SCHEMA", "dbo"),
    "sfWarehouse" -> getenv("SF_WAREHOUSE", "DELIVERY_WH")
  )

  @FunctionName("FixGetDepositCount")
  def fixGetDepositCount(
      @HttpTrigger(
        name = "req",
        methods = Array(HttpMethod.GET),
        authLevel = AuthorizationLevel.ADMIN
      )
      request: HttpRequestMessage[Optional[String]],
      context: ExecutionContext
  ): Unit = {
    implicit val logger = context.getLogger
    runDC(playnorth100, false)
    runDC(playnorth101, false)
    runDC(vegasonline, false)
  }

  @FunctionName("GetDepositCount")
  def runGetDepositCount(
      @TimerTrigger(
        name = "GetDepositCount",
        schedule = "0 0 10 * * *"
      ) timerInfo: String,
      context: ExecutionContext
  ): Unit = {
    implicit val logger = context.getLogger
    logger.info(s"starting app $timerInfo")

    runDC(playnorth100)
    runDC(playnorth101)
    runDC(vegasonline)
  }

  def runDC(b: Brand, withFilter: Boolean = true)(implicit logger: Logger) = {

    val filter = " AND DATEDIFF(day, LAST_UPDATE, GETDATE()) <= 7"
    val subQuery =
      s"select player_id as id, num_deposits as nb, deposits as total FROM bus.V_PLAYERS_LIFETIME_VALUES WHERE BRAND_ID ${b.filter}"
    val query = if(withFilter) {
      subQuery + filter
    } else {
      subQuery
    }
    val df = spark.read
      .format(net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME)
      .option("query", query)
      .options(sfOptions)
      .load()

    import spark.implicits._
    import functions._
    val newDf = df.select(
      df.col("id"),
      map(
        lit("lifetime_deposit_count"),
        $"nb",
        lit("lifetime_deposit_amount"),
        $"total"
      ).as("properties")
    )

    saveDFAndSendBatchNotif(
      newDf,
      b,
      "lifetime_deposit",
      "lifetime_deposit.json"
    )(logger)
  }

  @FunctionName("LastActivityDate")
  def runLastActivityDate(
      @TimerTrigger(
        name = "LastActivityDate",
        schedule = "0 1 10 * * *"
      ) timerInfo: String,
      context: ExecutionContext
  ): Unit = {
    implicit val logger = context.getLogger
    logger.info(s"starting app $timerInfo")
    runLastAD(playnorth100)
    runLastAD(playnorth101)
    runLastAD(vegasonline)
  }

  def runLastAD(b: Brand)(implicit logger: Logger) = {
    val query =
      s"""
          |SELECT player_id as id, last_activity_date 
          |FROM CENTRALDW.BUS.PLAYERS_LAST_ACTIVITY_DATE 
          |WHERE brand_id ${b.filter} AND DATEDIFF(day, LAST_ACTIVITY_DATE, GETDATE()) <= 7
          |""".stripMargin

    logger.info(query)
    val df = spark.read
      .format(net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME)
      .option("query", query)
      .options(sfOptions)
      .load()

    import spark.implicits._
    import functions._
    val newDf = df.select(
      df.col("id"),
      map(
        lit("last_activity_date"),
        $"last_activity_date"
      ).as("properties")
    )

    saveDFAndSendBatchNotif(
      newDf,
      b,
      "last_activity_date",
      "last_activity_date.json"
    )
  }

  @FunctionName("LastBetDatetime")
  def runLastBetDatetime(
      @TimerTrigger(
        name = "LastBetDatetime",
        schedule = "0 5 10 * * *"
      ) timerInfo: String,
      context: ExecutionContext
  ): Unit = {
    implicit val logger = context.getLogger
    logger.info(s"starting app $timerInfo")
    runLastBD(playnorth100)
    runLastBD(playnorth101)
    runLastBD(vegasonline)
  }

  def runLastBD(b: Brand)(implicit logger: Logger) = {
    val query =
      s"""
         |SELECT player_id  as id,
         | CASE p.brand_id WHEN ${vegasonline.id} THEN 
         |TO_VARCHAR(CONVERT_TIMEZONE('UTC', 'Europe/Budapest', max(bet_datetime)), 'yyyy-MM-dd') 
         | ELSE 
         | max(bet_datetime) 
         | END as last_bet_date
         |from dbo.fct_wagering w
         |INNER JOIN dbo.dim_players p on w.player_wid = p.player_wid AND p.brand_id = w.brand_id
         |where w.brand_id ${b.filter}
         |AND DATEDIFF(day, TRY_TO_DATE(TO_CHAR(w.pif_delta),'YYYYMMDDHHMISS'), GETDATE()) <= 8
         |group by player_id, p.brand_id
      """.stripMargin

    logger.info(query)
    val df = spark.read
      .format(net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME)
      .option("query", query)
      .options(sfOptions)
      .load()

    import spark.implicits._
    import functions._
    val newDf = df.select(
      df.col("id"),
      map(
        lit("last_bet_date"),
        $"last_bet_date"
      ).as("properties")
    )

    saveDFAndSendBatchNotif(
      newDf,
      b,
      "last_bet_date",
      "last_bet_date.json"
    )
  }

  @FunctionName("FixLastDepositDatetime")
  def fixLastLastDepositDatetime(
      @HttpTrigger(
        name = "req",
        methods = Array(HttpMethod.GET),
        authLevel = AuthorizationLevel.ADMIN
      )
      request: HttpRequestMessage[Optional[String]],
      context: ExecutionContext
  ): Unit = {
    implicit val logger = context.getLogger
    logger.info(s"starting app $request")
    runLastDepD(playnorth100, false)
    runLastDepD(playnorth101, false)
    runLastDepD(vegasonline, false)
  }

  @FunctionName("LastDepositDatetime")
  def runLastLastDepositDatetime(
      @TimerTrigger(
        name = "LastLastDepositDatetime",
        schedule = "0 15 10 * * *"
      ) timerInfo: String,
      context: ExecutionContext
  ): Unit = {
    implicit val logger = context.getLogger
    logger.info(s"starting app $timerInfo")
    runLastDepD(playnorth100)
    runLastDepD(playnorth101)
    runLastDepD(vegasonline)
  }

  def runLastDepD(b: Brand, withFilter: Boolean = true)(implicit
      logger: Logger
  ) = {
    val filter =
      " AND DATEDIFF(day, TRY_TO_DATE(TO_CHAR(LATEST_DEPOSIT),'YYYYMMDDHHMISS'), GETDATE()) <= 8 "
    val queryBase =
      s"""
         |SELECT player_id as id, TO_VARCHAR(LATEST_DEPOSIT, 'yyyy-MM-dd') as last_deposit_date
         |FROM CENTRALDW.BUS.V_LAST_DEPOSIT_WITHDRAWAL
         |WHERE brand_id ${b.filter} 
      """.stripMargin

    val query = if (withFilter) {
      queryBase + filter 
    } else {
      queryBase
    }

    logger.info(query)
    val df = spark.read
      .format(net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME)
      .option("query", query)
      .options(sfOptions)
      .load()

    import spark.implicits._
    import functions._
    val newDf = df.select(
      df.col("id"),
      map(
        lit("last_deposit_date"),
        $"last_deposit_date"
      ).as("properties")
    )

    saveDFAndSendBatchNotif(
      newDf,
      b,
      "last_deposit_date",
      "last_deposit_date.json"
    )
  }

  @FunctionName("CleanLastDepositDatetime")
  def cleanLastLastDepositDatetime(
      @HttpTrigger(
        name = "req",
        methods = Array(HttpMethod.GET),
        authLevel = AuthorizationLevel.ADMIN
      )
      request: HttpRequestMessage[Optional[String]],
      context: ExecutionContext
  ): Unit = {
    implicit val logger = context.getLogger
    logger.info(s"starting app $request")
    runLastDepositClean(playnorth100)
    runLastDepositClean(playnorth101)
    runLastDepositClean(vegasonline)
  }
  def runLastDepositClean(b: Brand)(implicit
      logger: Logger
  ) = {
    val query =
      s"""
      > SELECT players.PLAYER_ID as id, '' as last_deposit_date FROM DIM_PLAYERS players LEFT JOIN BUS.V_LAST_DEPOSIT_WITHDRAWAL i_dont_want
      > ON (players.player_id = i_dont_want.player_id AND players.brand_id = i_dont_want.brand_id )
      > WHERE i_dont_want.player_id IS NULL AND players.brand_id ${b.filter}
      """.stripMargin('>')


    logger.info(query)
    val df = spark.read
      .format(net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME)
      .option("query", query)
      .options(sfOptions)
      .load()

    import spark.implicits._
    import functions._
    val newDf = df.select(
      df.col("id"),
      map(
        lit("last_deposit_date"),
        $"last_deposit_date"
      ).as("properties")
    )

    saveDFAndSendBatchNotif(
      newDf,
      b,
      "clean_last_deposit_date",
      "clean_last_deposit_date.json"
    )
  }


  def runDateFix(
      timerInfo: String,
      context: ExecutionContext
  ): Unit = {
    implicit val logger = context.getLogger
    logger.info(s"starting app $timerInfo")
    runDateFixSub(vegasonline)
  }
  def runDateFixSub(b: Brand)(implicit logger: Logger) = {
    val query =
      s"""
         |SELECT player_id as id, TO_VARCHAR(CONVERT_TIMEZONE('UTC', 'Europe/Budapest', first_dep_datetime), 'yyyy-MM-dd') as first_dep_datetime,TO_VARCHAR(CONVERT_TIMEZONE('UTC', 'Europe/Budapest', dob), 'yyyy-MM-dd') as dob, TO_VARCHAR(CONVERT_TIMEZONE('UTC', 'Europe/Budapest', reg_datetime), 'yyyy-MM-dd') as reg_datetime 
         |FROM dim_players
         |WHERE brand_id ${b.filter}
      """.stripMargin

    logger.info(query)
    val df = spark.read
      .format(net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME)
      .option("query", query)
      .options(sfOptions)
      .load()

    import spark.implicits._
    import functions._
    val newDf = df.select(
      df.col("id"),
      map(
        lit("dob"),
        $"dob",
        lit("reg_datetime"),
        $"reg_datetime",
        lit("first_dep_datetime"),
        $"first_dep_datetime"
      ).as("properties")
    )

    saveDFAndSendBatchNotif(
      newDf,
      b,
      "budapest_timezone",
      "budapest_timezone.json"
    )
  }

  def saveDFAndSendBatchNotif(
      dataFrame: DataFrame,
      b: Brand,
      folderExtension: String,
      fileName: String
  )(implicit logger: Logger) = {
    import org.apache.hadoop.fs._

    val outputContainerPath =
      s"wasbs://$outputContainerName@$storageName.blob.core.windows.net"
    val dataFolder = if (dryRun) "test" else "data"

    val destinationFolder = s"$dataFolder/${b.folder}/$folderExtension"
    val outputBlobFolder = s"$outputContainerPath/$destinationFolder"

    dataFrame
      .coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .mode("overwrite")
      .option("quote", " ")
      .option("escapeQuotes", "false")
      .option("escape", " ")
      .json(s"$outputBlobFolder")

    val file =
      fs.globStatus(new Path(s"$outputBlobFolder/*.json"))(0).getPath.getName

    fs.rename(
      new Path(s"$outputBlobFolder/$file"),
      new Path(s"$outputBlobFolder/$fileName")
    )

    val client = new BlobServiceClientBuilder()
      .connectionString(
        s"DefaultEndpointsProtocol=https;AccountName=centraliseddwa6a1;AccountKey=$storageAccessKey;EndpointSuffix=core.windows.net"
      )
      .buildClient()
    val blobClient = client
      .getBlobContainerClient(outputContainerName)
      .getBlobClient(s"$destinationFolder/$fileName")
    val blobSasPermission = new BlobSasPermission().setReadPermission(true)
    val expiryTime = OffsetDateTime.now().plusDays(1)
    val values =
      new BlobServiceSasSignatureValues(expiryTime, blobSasPermission)
        .setStartTime(OffsetDateTime.now())

    val url = s"${blobClient.getBlobUrl}?${blobClient.generateSas(values)}"
    logger.info(url)

    b.sendNotification(dryRun, url)
  }

}

class Brand(
    val id: Int,
    val folder: String,
    val queue: String,
    region: Regions
) {

  val filter = s" = $id"

  lazy val sqsClient = AmazonSQSClientBuilder
    .standard()
    .withRegion(region)
    .withCredentials(
      new EnvironmentVariableCredentialsProvider()
    )
    .build()

  def sendNotification(dryRun: Boolean, downloadUrl: String)(implicit
      logger: Logger
  ): Unit = {

    val mapping_selector_prefix = if (id == 100 || id == 101) s"${id}_" else ""
    val body =
      s"""
         |{
         |  "type": "BATCH_NOTIFICATION",
         |  "mappingSelector": "${mapping_selector_prefix}batch-import",
         |  "downloadUri": "$downloadUrl"
         |}
         |""".stripMargin

    val request = new SendMessageRequest()
      .withQueueUrl(queue)
      .withMessageBody(body)
      .withMessageGroupId("finnplay")
      .withMessageDeduplicationId(s"${Instant.now().toEpochMilli}")

    try {
      if (dryRun) {
        logger.info(s"dry run $body in $downloadUrl")
      } else {
        Option(sqsClient.sendMessage(request)) foreach { result =>
          logger.info(s"Sent queue message: $body => result: $result")
        }
      }
    } catch {
      case e: Exception =>
        logger.info(s"wasn't able to send notification. Reason $e")
    }
  }
}
