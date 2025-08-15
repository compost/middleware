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
object PrivateKey {
  import java.security.PrivateKey
  import java.util.Base64
  import java.security.Security
  import org.bouncycastle.jce.provider.BouncyCastleProvider
  import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo
  import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder
  import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter
  import org.bouncycastle.openssl.PEMParser
  import java.io.StringReader

  def get: PrivateKey = {
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

  def getPem(): String = {
    val encodedPrivateKey = get.getEncoded();
    val base64EncodedPrivateKey =
      Base64.getEncoder().encodeToString(encodedPrivateKey);

    val pemBuilder = new StringBuilder();
    pemBuilder.append(base64EncodedPrivateKey);

    return pemBuilder.toString();
  }
}

object Functions {
  def getenv(key: String, defaultValue: String): String = {
    Option(System.getenv(key)).getOrElse(defaultValue)
  }
  val topic = getenv("TOPIC", "boom")
  val table = getenv("TABLE", "boom")
  val kafkaBrokers = getenv("KAFKA_BROKERS", "127.0.0.1:9092")
  val storageAccessKey = getenv("STORAGE_ACCESS_KEY", "NOT_HERE")
  val storageName = getenv("STORAGE_NAME", "TODO")
  val outputContainerName = getenv("OUTPUT_CONTAINER_NAME", "todo")

  val sfOptions = Map(

    "pem_private_key" -> PrivateKey.getPem(),
    "sfURL" -> getenv("SF_URL", "irzzdfb-bj65063.snowflakecomputing.com"),
    "sfUser" -> getenv("SF_USER", "APP_MIDDLEWARE_SERVICE"),
    "sfDatabase" -> getenv("SF_DATABASE", "CENTRALDW"),
    "sfSchema" -> getenv("SF_SCHEMA", "BUS"),
    "sfWarehouse" -> getenv("SF_WAREHOUSE", "DELIVERY_WH")
  )

}
class Functions {
  import Functions._
  val lucky7 = new Brands(
    ids = Set(192),
    folder = "spinaway2", // lucky7
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/raging-rhino-nv-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val lucky7Dev = new Brands(
    ids = Set(193),
    folder = "dev-spinaway2", // lucky7
    mappingSelector = "DEV_batch-import",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/raging-rhino-nv-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val all = Set[Brands](lucky7, lucky7Dev)

  lazy val spark = SparkSession.builder
    .master("local[*]")
    .appName("gig")
    .getOrCreate()
  val connectionStringBlob =
    s"DefaultEndpointsProtocol=https;AccountName=$storageName;AccountKey=$storageAccessKey;EndpointSuffix=core.windows.net"

  spark.conf.set(
    s"fs.azure.account.key.$storageName.blob.core.windows.net",
    storageAccessKey
  )

  lazy val fs = Try({
    val configObj = new Configuration()
    configObj.set(
      "fs.defaultFS",
      s"wasbs://$outputContainerName@$storageName.blob.core.windows.net"
    )
    configObj.set(
      "fs.azure.account.key." + storageName + ".blob.core.windows.net",
      storageAccessKey
    )
    FileSystem.newInstance(configObj)
  }).getOrElse(throw new Exception(s"Cannot access storage"))

  val configObj = new Configuration()
  configObj.set(
    "fs.defaultFS",
    s"wasbs://$outputContainerName@$storageName.blob.core.windows.net"
  )
  configObj.set(
    "fs.azure.account.key." + storageName + ".blob.core.windows.net",
    storageAccessKey
  )

  @FunctionName("lastPlayedDate")
  def lastPlayedDate(
      @TimerTrigger(
        name = "lastPlayedDate",
        schedule = "0 25 7 * * *"
      ) timerInfo: String,
      context: ExecutionContext
  ): Unit = {
    implicit val logger = context.getLogger
    logger.info(s"starting app $timerInfo")
    all.foreach(b =>
      runSub(
        b,
        "lastPlayedDate",
        s"""
        | SELECT TO_JSON(OBJECT_CONSTRUCT('id', p.PLAYER_ID, 'properties', OBJECT_CONSTRUCT('last_played_date', to_varchar(max(a.bet_datetime), 'YYYY-MM-DD')))) as value
        | FROM DBO.FCT_WAGERING a
        | INNER JOIN DBO.DIM_PLAYERS p ON p.PLAYER_WID=a.PLAYER_WID AND a.BRAND_ID =p.BRAND_ID
        | INNER JOIN DBO.dim_brands b ON a.brand_id=b.BRAND_ID
        | WHERE b.brand_id ${b.filter} AND a.TRANSACTION_SUBTYPE_ID = 13
        | GROUP BY p.player_id
      """.stripMargin
      )
    // AND DATEDIFF(day, TRY_TO_DATE(TO_CHAR(a.pif_delta),'YYYYMMDDHHMISS'), GETDATE()) <= 8
    )
  }

  @FunctionName("consentHistory")
  def consentHistory(
      @TimerTrigger(
        name = "consentHistory",
        schedule = "0 25 7 * * *"
      ) timerInfo: String,
      context: ExecutionContext
  ): Unit = {
    implicit val logger = context.getLogger
    logger.info(s"starting app $timerInfo")
    all.foreach(b =>
      runSub(
        b,
        "consentHistory",
        s"""
        | SELECT TO_JSON(OBJECT_CONSTRUCT('id', PLAYER_ID, 'properties', 
        | OBJECT_CONSTRUCT('consent_marketingemail', lower(CONSENT_MARKETING_EMAIL),'consent_marketingtextmessage', 
        | lower(CONSENT_MARKETING_TEXT_MESSAGE), 'consent_marketingdirectmail', lower(CONSENT_MARKETING_DIRECT_MAIL),
        | 'consent_marketingtelephone', lower(CONSENT_MARKETING_TELEPHONE), 'consent_marketingoms', lower(CONSENT_MARKETING_OMS) ))) as value
        | FROM BUS.RAW_CRM_IMPORT_PLAYER_CONSENT_L7
        | WHERE brand_id ${b.filter} 
      """.stripMargin,
        false
      )
    // AND DATEDIFF(day, TRY_TO_DATE(TO_CHAR(a.pif_delta),'YYYYMMDDHHMISS'), GETDATE()) <= 8
    )
  }

  def runSub(
      b: Brands,
      name: String,
      query: String,
      send: Boolean = true
  )(implicit logger: java.util.logging.Logger): Unit = {
    logger.info(query)
    val df = spark.read
      .format(net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME)
      .option(
        "query",
        query
      )
      .options(sfOptions)
      .load()
    saveDFAndSendBatchNotif(df, b, name, send)
  }

  def saveDFAndSendBatchNotif(
      dataFrame: DataFrame,
      b: Brands,
      kindOfData: String,
      send: Boolean = true
  )(implicit logger: Logger) = {
    import org.apache.hadoop.fs._

    val fileName = s"${kindOfData}.json"

    val outputContainerPath =
      s"wasbs://$outputContainerName@$storageName.blob.core.windows.net"
    val dataFolder = if (send) "data" else "test"

    val destinationFolder = s"$dataFolder/${b.folder}/$kindOfData"
    val outputBlobFolder = s"$outputContainerPath/$destinationFolder"

    dataFrame
      .coalesce(1)
      .write
      .format("text")
      .mode("overwrite")
      .save(outputBlobFolder)

    val file =
      fs.globStatus(new Path(s"$outputBlobFolder/*.txt"))(0).getPath.getName

    fs.rename(
      new Path(s"$outputBlobFolder/$file"),
      new Path(s"$outputBlobFolder/$fileName")
    )

    val client = new BlobServiceClientBuilder()
      .connectionString(connectionStringBlob)
      .buildClient()
    val blobClient = client
      .getBlobContainerClient(outputContainerName)
      .getBlobClient(s"$destinationFolder/$fileName")
    val blobSasPermission = new BlobSasPermission().setReadPermission(true)
    val expiryTime = OffsetDateTime.now().plusDays(7)
    val values =
      new BlobServiceSasSignatureValues(expiryTime, blobSasPermission)
        .setStartTime(OffsetDateTime.now())

    val url = s"${blobClient.getBlobUrl}?${blobClient.generateSas(values)}"
    logger.info(url)
    b.sendNotification(send, url)
  }

  @FunctionName("produceBlocked")
  def runProduceBlocked(
      @HttpTrigger(
        name = "req",
        methods = Array(HttpMethod.GET),
        authLevel = AuthorizationLevel.ADMIN
      )
      request: HttpRequestMessage[Optional[String]],
      context: ExecutionContext
  ): Unit = {
    implicit val logger = Option(context)
      .map(_.getLogger)
      .getOrElse(java.util.logging.Logger.getGlobal())
    logger.info(s"starting app $request")
    val df = spark.read
      .format(net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME)
      .option(
        "query",
        s"""
          |select brand_id || '-' || player_id as key, TO_JSON(OBJECT_CONSTRUCT(*)) as value
          |from BUS.RAW_CRM_IMPORT_PLAYER_BLOCKS_L7 join (select 'true' as blocked) where brand_id is not null and player_id is not null
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
      .option("topic", "history-blocked")
      .save()
  }

  @FunctionName("produceConsent")
  def runProduceConsent(
      @HttpTrigger(
        name = "req",
        methods = Array(HttpMethod.GET),
        authLevel = AuthorizationLevel.ADMIN
      )
      request: HttpRequestMessage[Optional[String]],
      context: ExecutionContext
  ): Unit = {
    implicit val logger = Option(context)
      .map(_.getLogger)
      .getOrElse(java.util.logging.Logger.getGlobal())
    logger.info(s"starting app $request")
    val df = spark.read
      .format(net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME)
      .option(
        "query",
        s"""
          |select brand_id || '-' || player_id as key, TO_JSON(OBJECT_CONSTRUCT(*)) as value
          |from BUS.RAW_CRM_IMPORT_PLAYER_CONSENT_L7 where brand_id is not null and player_id is not null
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
      .option("topic", "history-consent")
      .save()
  }
}
