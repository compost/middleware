package com.jada.base

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
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import java.time.{Instant, OffsetDateTime}
import java.util.logging.Logger
import scala.util.Try
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
class Functions {

  val Deposit = 1
  val Complete = 2

  val slotbox = new Brand(
    id = 297,
    folder = "slotbox",
    mappingSelector = "batch-import",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/eventhub_two_2663_slotbox.fifo",
    region = Regions.EU_CENTRAL_1
  )


  lazy val spark = SparkSession.builder
    .master("local[*]")
    .appName("BaseGetBatchUpdate")
    .config("spark.driver.memory", "1G")
    .getOrCreate()

  val storageName = "centraliseddwa6a1"
  val storageAccessKey = System.getenv("STORAGE_ACCESS_KEY")
  val ids = System.getenv("BRAND_IDS").split(",").map(_.toInt)
  val all =
    Set(
      slotbox,
    ).filter(b => ids.contains(b.id))

  val outputContainerName = "base"
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

  @FunctionName("fix")
  def fix(
      @HttpTrigger(
        name = "req",
        methods = Array(HttpMethod.GET),
        authLevel = AuthorizationLevel.ADMIN
      )
      request: HttpRequestMessage[Optional[String]],
      context: ExecutionContext
  ): Unit = {
    implicit val logger = context.getLogger
    logger.info(s"starting app $context")

    val table = "CENTRALDW.BUS.RAW_CRM_IMPORT_WISEGAMING"
    val properties = Map[String, String](
      "player_id" -> "ORIGINALID",
      "brand_id" -> "BRAND_ID",
      "is_blocked" -> "IFF(is_blocked = TRUE, 1, 0) ",
      "abuser_role_tag" -> "IFF(abuser_role_tag = TRUE, 1, 0) ",
      "is_self_excluded" -> "IFF(is_self_excluded = TRUE, 1, 0) ",
      "consent_marketingtelephone" -> "IFF(consent_marketingtelephone = TRUE, 1, 0) ",
      "consent_marketingdirectmail" -> "IFF(consent_marketingdirectmail = TRUE, 1, 0) ",
      "consent_marketingoms" -> "IFF(consent_marketingoms = TRUE, 1, 0) ",
      "consent_EMAIL" -> "IFF(consent_email = TRUE, 1, 0) ",
      "consent_email" -> "IFF(consent_email = TRUE, 1, 0) ",
    ).map { case (k, v) => s"'$k', $v" }.mkString(",")
    val name = "fix-boolean"
    all.foreach(fixSub(_, name, table, properties, columnPlayerId = "ORIGINALID"))
  }

  def fixSub(
      brand: Brand,
      name: String,
      table: String,
      properties: String,
      columnPlayerId: String = "PLAYER_ID",
      columnBrandId: String = "BRAND_ID"
  )(implicit logger: java.util.logging.Logger): Unit = {
    val sql = s"""
          |SELECT TO_JSON(OBJECT_CONSTRUCT('id', ${columnPlayerId}, 'properties',  OBJECT_CONSTRUCT(${properties}))) 
          |FROM ${table}
          |WHERE ${columnBrandId} ${brand.filter}
          |""".stripMargin
    logger.info(sql)
    val df = spark.read
      .format(net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME)
      .option(
        "query",
        sql
      )
      .options(sfOptions)
      .load()

    import spark.implicits._
    import functions._
    saveDFAndSendBatchNotif(
      df,
      brand,
      name,
      s"$name.json",
      format = "txt"
    )
  }
  def saveDFAndSendBatchNotif(
      dataFrame: DataFrame,
      b: Brand,
      folderExtension: String,
      fileName: String,
      format: String = "json",
      send: Boolean = true
  )(implicit logger: Logger) = {
    import org.apache.hadoop.fs._

    val outputContainerPath =
      s"wasbs://$outputContainerName@$storageName.blob.core.windows.net"
    val dataFolder = if (send) "test" else "data"

    val destinationFolder =
      s"$dataFolder/${b.folder}/$folderExtension"
    val outputBlobFolder = s"$outputContainerPath/$destinationFolder"

    if (format == "json") {
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
    } else {
      dataFrame
        .coalesce(1)
        .write
        .format("text")
        .mode("overwrite")
        .save(outputBlobFolder)

    }
    val file =
      fs.globStatus(new Path(s"$outputBlobFolder/*.$format"))(0).getPath.getName

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

    b.sendNotification(send, url)

  }

}

class Brand(
    val id: Int,
    val folder: String,
    val queue: String,
    val mappingSelector: String = "batch-import",
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

  def sendNotification(send: Boolean, downloadUrl: String)(implicit
      logger: Logger
  ): Unit = {

    val body =
      s"""
         |{
         |  "type": "BATCH_NOTIFICATION",
         |  "mappingSelector": "${mappingSelector}",
         |  "downloadUri": "$downloadUrl"
         |}
         |""".stripMargin

    val request = new SendMessageRequest()
      .withQueueUrl(queue)
      .withMessageBody(body)
      .withMessageGroupId("base")
      .withMessageDeduplicationId(s"${Instant.now().toEpochMilli}")

    try {
      if (send) {
        Option(sqsClient.sendMessage(request)) foreach { result =>
          logger.info(s"Sent queue message: $body => result: $result")
        }
      } else {
        logger.info(s"dry run $body in $downloadUrl")

      }
    } catch {
      case e: Exception =>
        logger.info(s"wasn't able to send notification. Reason $e")
    }
  }
}
