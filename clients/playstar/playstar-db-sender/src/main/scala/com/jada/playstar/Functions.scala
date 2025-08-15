package com.jada.playstar

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider
import com.amazonaws.regions.Regions
import com.amazonaws.services.sqs.{AmazonSQS, AmazonSQSClientBuilder}
import com.amazonaws.services.sqs.model.SendMessageRequest
import com.azure.storage.blob.BlobServiceClientBuilder
import com.azure.storage.blob.implementation.util.ModelHelper
import com.azure.storage.blob.sas.{BlobSasPermission, BlobServiceSasSignatureValues}
import com.microsoft.azure.functions._
import com.microsoft.azure.functions.annotation.{FunctionName, TimerTrigger}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import java.time.{Instant, OffsetDateTime}
import java.util.logging.Logger
import scala.util.Try

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
class Functions {

  val playstar = new Brand(
    id = 8,
    folder = "playstar",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/playstar-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )

  lazy val spark = SparkSession.builder
    .master("local[*]")
    .appName("playstarGetBatchUpdate")
    .config("spark.driver.memory", "1G")
    .getOrCreate()

  val dryRun = Option(System.getenv("DRY_RUN")).map(_.toBoolean).getOrElse(true)
  val storageName = "centraliseddwa6a1"
  val storageAccessKey = System.getenv("STORAGE_ACCESS_KEY")
  val outputContainerName = "playstar"
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


  def getenv(key: String, defaultValue: String): String = {
    Option(System.getenv(key)).getOrElse(defaultValue)
  }

  val sfOptions = Map(
    "pem_private_key" -> PrivateKey.getPem(),
    "sfURL" -> getenv("SF_URL", "irzzdfb-bj65063.snowflakecomputing.com"),
    "sfUser" -> getenv("SF_USER", "APP_MIDDLEWARE_SERVICE"),
    "sfDatabase" -> getenv("SF_DATABASE", "CENTRALDW"),
    "sfSchema" -> getenv("SF_SCHEMA", "dbo"),
    "sfWarehouse" -> getenv("SF_WAREHOUSE", "DELIVERY_WH")
  )
  @FunctionName("GetCasinoLastPlayedDate")
  def runGetDepositCount(
      @TimerTrigger(
        name = "GetCasinoLastPlayedDate",
        schedule = "0 5 10 * * *"
      ) timerInfo: String,
      context: ExecutionContext
  ): Unit = {
    implicit val logger = context.getLogger
    logger.info(s"starting app $timerInfo")

    runLPCD(playstar)
  }

  def runLPCD(b: Brand)(implicit logger: Logger) = {
    val query =
      s"""
         | select player_id as id, to_varchar(max(CONVERT_TIMEZONE('UTC', 'America/New_York',bet_datetime)), 'yyyy-MM-dd') as casino_last_played_datetime
         | from dbo.fct_wagering w
         | inner join dbo.dim_players p on w.brand_id = p.brand_id and w.player_wid = p.player_wid
         | where w.brand_id ${b.filter} and w.vertical_wid = 120
         | and datediff(day, try_to_date(to_char(w.pif_delta),'YYYYMMDDHHMISS'), getdate()) <= 8 and transaction_subtype_id = 13
         | group by player_id
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
        lit("casino_last_played_datetime"),
        $"casino_last_played_datetime"
      ).as("properties")
    )

    saveDFAndSendBatchNotif(
      newDf,
      b,
      "casino_last_played_datetime",
      "casino_last_played_datetime.json"
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

    val body =
      s"""
         |{
         |  "type": "BATCH_NOTIFICATION",
         |  "mappingSelector": "batch_import",
         |  "downloadUri": "$downloadUrl"
         |}
         |""".stripMargin

    val request = new SendMessageRequest()
      .withQueueUrl(queue)
      .withMessageBody(body)
      .withMessageGroupId("playstar")
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

