package com.jada.soft2bet

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
import java.util.UUID

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
class Brands(
    val ids: Set[Int],
    val folder: String,
    val queue: String,
    region: Regions
) {

  val filter = s" IN (${ids.mkString(",")})"

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
         |  "mappingSelector": "batch-import",
         |  "downloadUri": "$downloadUrl"
         |}
         |""".stripMargin

    val request = new SendMessageRequest()
      .withQueueUrl(queue)
      .withMessageBody(body)
      .withMessageGroupId("soft2bet")
      .withMessageDeduplicationId(s"${UUID.randomUUID().toString()}")

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
class Functions {

  val mx = new Brands(
    ids = Set(233, 289),
    folder = "mx",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/soft2bet-mx-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val sp = new Brands(
    ids = Set(327),
    folder = "sp",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/eventhub_three_5311_medierES.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val ibet = new Brands(
    ids = Set(216, 244, 226, 41, 286),
    folder = "mx",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/eventhub_three_5308_medierIbet.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val sq = new Brands(
    ids = Set(255, 274),
    folder = "sq",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/eventhub_three_5305_soft2betSquibo.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val ds = new Brands(
    ids = Set(),
    folder = "ds",
    queue =
      "https://eu-central-1.queue.amazonaws.com/663880797555/eventhub_three_5304_soft2betDs.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val spin247 = new Brands(
    ids = Set(150, 249),
    folder = "spin247",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/soft2bet-spin247-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )
  val sga = new Brands(
    ids = Set(56, 76, 77, 129, 314),
    folder = "sga",
    queue =
      "https://sqs.eu-north-1.amazonaws.com/663880797555/soft2bet-sga-datahub-eventhub.fifo",
    region = Regions.EU_NORTH_1
  )
  val dk = new Brands(
    ids = Set(57, 213),
    folder = "dk",
    queue =
      "https://sqs.eu-north-1.amazonaws.com/663880797555/soft2bet-dk-datahub-eventhub.fifo",
    region = Regions.EU_NORTH_1
  )
  val funid = new Brands(
    ids = Set(214, 321),
    folder = "funid",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/soft2bet-funid-5290-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val nb = new Brands(
    ids = Set(),
    folder = "nb",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/soft2bet-nb-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )
  val fp = new Brands(
    ids = Set(229, 230, 236, 237, 253, 258, 272, 273, 282, 285, 300, 311, 320),
    folder = "fp",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/soft2bet-fp-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )
  val mga = new Brands(
    ids = Set(53, 54, 55, 64, 65, 66, 67, 109, 111, 259),
    folder = "mga",
    queue =
      "https://sqs.eu-north-1.amazonaws.com/663880797555/soft2bet-mga-datahub-eventhub.fifo",
    region = Regions.EU_NORTH_1
  )

  val other = new Brands(
    ids = Set(81, 43, 2, 34, 47, 52, 37, 85, 18, 27, 45, 39, 48, 5, 29, 28, 87,
      32, 26, 36, 46, 84, 33, 16, 31, 19, 42, 35, 20, 38, 40, 50, 51, 25, 49,
      17, 6, 44, 78, 107, 30, 108, 122, 125, 63, 128, 134, 147, 148, 149, 151,
      152, 181, 183, 80, 184, 185, 196, 197, 199, 200, 201, 202, 203, 204, 217,
      218, 219, 220, 221, 222, 224, 225, 227, 228, 234, 235, 240, 243, 245, 246,
      247, 248, 250, 254, 256, 257, 261, 262, 267, 268, 269, 270, 271, 232, 280,
      281, 283, 284, 287, 288, 293, 298, 299, 302, 303, 304, 305, 306, 307, 110,
      198, 310, 260, 316, 317, 318, 319, 322, 323, 325, 326),
    folder = "other",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/soft2bet-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val ca = new Brands(
    ids = Set(182, 324),
    folder = "ca",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/soft2bet-ca-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val casinoinfinity = new Brands(
    ids = Set(290, 301),
    folder = "casinoinfinity",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/soft2bet-casinoinfinity-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val boomerang = new Brands(
    ids = Set(123, 124, 153, 133, 223, 308),
    folder = "boomerang",
    queue =
      "https://sqs.eu-north-1.amazonaws.com/663880797555/soft2bet-boomerang-datahub-eventhub.fifo",
    region = Regions.EU_NORTH_1
  )

  val ro = new Brands(
    ids = Set(86, 135, 315),
    folder = "ro",
    queue =
      "https://sqs.eu-north-1.amazonaws.com/663880797555/soft2bet-ro-datahub-eventhub.fifo",
    region = Regions.EU_NORTH_1
  )
  val ro2 = new Brands(
    ids = Set(309),
    folder = "ro2",
    queue =
      "https://sqs.eu-north-1.amazonaws.com/663880797555/eventhub_three_5310_soft2betRo2.fifo",
    region = Regions.EU_NORTH_1
  )
  val elabet = new Brands(
    ids = Set(215),
    folder = "elabet",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/soft2bet-gr-datahub-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val cp = new Brands(
    ids = Set(252),
    folder = "cp",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/soft2bet-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )
  val all =
    Set(
      ds,
      dk,
      mga,
      sga,
      other,
      boomerang,
      casinoinfinity,
      ro,
      ro2,
      ca,
      fp,
      elabet,
      cp,
      funid,
      spin247,
      mx,
      nb,
      sq,
      ibet,
      sp
    ).filterNot(_.ids.isEmpty)

  def getenv(key: String, defaultValue: String): String = {
    Option(System.getenv(key)).getOrElse(defaultValue)
  }
  val storageAccessKey = getenv("STORAGE_ACCESS_KEY", "NOT_HERE")
  val sfOptions = Map(
    "pem_private_key" -> PrivateKey.getPem(),
    "sfURL" -> getenv("SF_URL", "irzzdfb-bj65063.snowflakecomputing.com"),
    "sfUser" -> getenv("SF_USER", "APP_MIDDLEWARE_SERVICE"),
    "sfDatabase" -> getenv("SF_DATABASE", "CENTRALDW"),
    "sfSchema" -> getenv("SF_SCHEMA", "dbo"),
    "sfWarehouse" -> getenv("SF_WAREHOUSE", "DELIVERY_WH")
  )
  val storageName = "centraliseddwa6a1"
  val outputContainerName = "soft2bet"

  lazy val spark = SparkSession.builder
    .master("local[*]")
    .appName("soft2betGetBatchUpdate")
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

  @FunctionName("FixCurrency")
  def runFixCurrency(
      @HttpTrigger(
        name = "req",
        methods = Array(HttpMethod.GET),
        authLevel = AuthorizationLevel.ADMIN
      )
      request: HttpRequestMessage[Optional[String]],
      context: ExecutionContext
  ): Unit = {
    implicit val logger = context.getLogger
    all.foreach(runSoft2betGetCurrencySub(_, false))

  }
  @FunctionName("Soft2betGetCurrency")
  def runSoft2betGetCurrency(
      @TimerTrigger(
        name = "Soft2betGetCurrency",
        schedule = "0 25 7 * * *"
      ) timerInfo: String,
      context: ExecutionContext
  ): Unit = {
    implicit val logger = context.getLogger
    logger.info(s"starting app $timerInfo")
    all.foreach(runSoft2betGetCurrencySub(_))
  }

  def runSoft2betGetCurrencySub(
      b: Brands,
      dateDiff: Boolean = true
  )(implicit logger: java.util.logging.Logger): Unit = {
    val mainQuery = s"""
          | SELECT player_id as id, c.currency_description as currency
          |FROM dbo.dim_players p
          | INNER JOIN dbo.dim_brands b ON p.brand_id = b.brand_id
          |inner join (
          |    select distinct currency_id as currency_id, currency_description, brand_id
          |    from dbo.dim_currency where currency_id is not null and currency_description is not null
          |    ) as c on p.currency_id = c.currency_id  and p.brand_id = c.brand_id
          |WHERE b.brand_id ${b.filter} 
          |""".stripMargin
    val diff = """
          | AND (
          |    (DATEDIFF(day, TRY_TO_DATE(TO_CHAR(p.pif_insert),'YYYYMMDDHHMISS'), GETDATE()) <= 8)
          |    OR
          |    (DATEDIFF(day, TRY_TO_DATE(TO_CHAR(p.pif_update),'YYYYMMDDHHMISS'), GETDATE()) <= 8)
          |)
          |
          |""".stripMargin

    val query = if (dateDiff) {
      mainQuery + diff
    } else {
      mainQuery
    }
    logger.info(query)
    val df = spark.read
      .format(net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME)
      .option(
        "query",
        query
      )
      .options(sfOptions)
      .load()

    import spark.implicits._
    import functions._
    val newDf = df.select(
      df.col("id"),
      map(lit("currency"), $"currency").as("properties")
    )
    saveDFAndSendBatchNotif(newDf, b, "currency")
  }

  @FunctionName("Soft2betGetLastActivityDate")
  def runSoft2betGetLastActivityDate(
      @TimerTrigger(
        name = "Soft2betSoft2betGetLastActivityDate",
        schedule = "0 0 10 * * *"
      ) timerInfo: String,
      context: ExecutionContext
  ): Unit = {
    implicit val logger = context.getLogger
    logger.info(s"starting app $timerInfo")

    all.foreach(runSoft2betGetLastActivityDateSub(_))
  }

  def runSoft2betGetLastActivityDateSub(
      b: Brands
  )(implicit logger: java.util.logging.Logger): Unit = {

    val df = spark.read
      .format(net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME)
      .option(
        "query",
        s"""
          |SELECT player_id as id, last_activity_date 
          |FROM CENTRALDW.BUS.PLAYERS_LAST_ACTIVITY_DATE 
          |WHERE brand_id ${b.filter} AND DATEDIFF(day, LAST_ACTIVITY_DATE, GETDATE()) <= 7
          |""".stripMargin
      )
      .options(sfOptions)
      .load()

    import spark.implicits._
    import functions._
    val newDf = df.select(
      df.col("id"),
      map(lit("last_activity_date"), $"last_activity_date").as("properties")
    )
    saveDFAndSendBatchNotif(newDf, b, "last_activity_date")
  }

  @FunctionName("Fix")
  def runFix(
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

    val b: Brands = funid
    val df = spark.read
      .format(net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME)
      .option(
        "query",
        s"""
          |SELECT id, '214' as brandid 
          |FROM VALUES 
         | as t(id) 
          | WHERE id like 'FUNID_%'
          |""".stripMargin
      )
      .options(sfOptions)
      .load()

    import spark.implicits._
    import functions._
    val newDf = df.select(
      df.col("id"),
      map(lit("brandid"), $"brandid").as("properties")
    )
    saveDFAndSendBatchNotif(newDf, b, "fix_fund_id", send = true)
  }

  @FunctionName("loadHistory")
  def runLoadHistory(
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

    val table = "BUS.SOFT2BET_PLAYER_SEGMENTATION_HISTORY"
    val name = "player-segmentation"
    val columnPlayerId = "player_id"
    val columnBrandId = "brand_id"
    val properties = Map[String, String](
      "RFM_APR_ACTIVITY" -> "RFM_APR_ACTIVITY",
      "RFM_APR_VALUE" -> "RFM_APR_VALUE",
      "RFM_APR_MONETARY" -> "RFM_APR_MONETARY",
      "RFM_APR_ORIENTATION" -> "RFM_APR_ORIENTATION",
      "RFM_APR_NEW_SEGMENT" -> "RFM_APR_NEW_SEGMENT",
      "RFM_APR_RUNDAY" -> "RFM_APR_RUNDAY"
    ).map { case (k, v) => s"'$k', $v" }.mkString(",")

    all.foreach(
      missingData(_, name, columnPlayerId, columnBrandId, table, properties)
    )
  }

  def missingData(
      brand: Brands,
      name: String,
      columnPlayerId: String,
      columnBrandId: String,
      table: String,
      properties: String
  )(implicit logger: java.util.logging.Logger): Unit = {
    val sql = s"""
          |SELECT TO_JSON(OBJECT_CONSTRUCT('id', ${columnPlayerId}, 'properties',  OBJECT_CONSTRUCT(${properties}))) 
          |FROM ${table}
          |WHERE $columnBrandId ${brand.filter} 
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
      format = "txt"
    )
  }

  @FunctionName("Soft2betGetLastDepositAmounts")
  def runSoft2betGetLastDepositAmounts(
      @TimerTrigger(
        name = "Soft2betGetLastDepositAmounts",
        schedule = "0 50 7 * * *"
      ) timerInfo: String,
      context: ExecutionContext
  ): Unit = {
    implicit val logger = context.getLogger
    logger.info(s"starting app $timerInfo")

    all.foreach(runSoft2betGetLastDepositAmountsSub(_))
  }

  def runSoft2betGetLastDepositAmountsSub(
      b: Brands
  )(implicit logger: java.util.logging.Logger): Unit = {

    val df = spark.read
      .format(net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME)
      .option(
        "query",
        s"""
           |WITH lastDeposits AS
           |  (SELECT w.player_wid,
           |          player_id,
           |          w.brand_id,
           |          max(w.transaction_datetime) AS max_date
           |   FROM (
           |     select distinct w.player_wid, w.brand_id
           |     from dbo.fct_wallet w
           |     where w.brand_id ${b.filter}
           |          AND DATEDIFF(day, TRY_TO_DATE(TO_CHAR(w.pif_delta),'YYYYMMDDHHMISS'), GETDATE()) <= 8
           | ) players_with_recent_deposit
           | inner join dbo.fct_wallet w on players_with_recent_deposit.player_wid = w.player_wid and players_with_recent_deposit.brand_id = w.brand_id
           | INNER JOIN dbo.dim_players p ON w.player_wid = p.player_wid AND p.brand_id = w.brand_id
           | inner join dbo.dim_transaction_subtypes st on st.transaction_subtype_id = w.transaction_subtype_id and st.brand_id = w.brand_id
           | inner join dbo.dim_transaction_status status on status.transaction_status_id = w.transaction_status_id and w.brand_id = status.brand_id
           | WHERE (transaction_status_description ='complete' OR transaction_status_description = 'accepted')  AND transaction_type_id =  1
           |GROUP BY w.player_wid,
           |          player_id,
           |          w.brand_id)
           |SELECT fd.player_id as id, w.amount as lastdepositamount
           |FROM lastDeposits fd
           |INNER JOIN dbo.fct_wallet w on fd.player_wid = w.player_wid and fd.brand_id = w.brand_id and fd.max_date = w.transaction_datetime
           | inner join dbo.dim_transaction_subtypes st on st.transaction_subtype_id = w.transaction_subtype_id and st.brand_id = w.brand_id
           | inner join dbo.dim_transaction_status status on status.transaction_status_id = w.transaction_status_id and w.brand_id = status.brand_id
           | WHERE (transaction_status_description ='complete' OR transaction_status_description = 'accepted')  AND transaction_type_id =  1
           |
           |""".stripMargin
      )
      .options(sfOptions)
      .load()

    import spark.implicits._
    import functions._
    val newDf = df.select(
      df.col("id"),
      map(lit("lastdepositamount"), $"lastdepositamount").as("properties")
    )
    saveDFAndSendBatchNotif(newDf, b, "lastdepositamount")
  }

  def saveDFAndSendBatchNotif(
      dataFrame: DataFrame,
      b: Brands,
      kindOfData: String,
      format: String = "json",
      send: Boolean = true
  )(implicit logger: Logger) = {
    import org.apache.hadoop.fs._

    val fileName = s"${kindOfData}.json"

    val outputContainerPath =
      s"wasbs://$outputContainerName@$storageName.blob.core.windows.net"
    val dataFolder = if (send) "test" else "data"

    val destinationFolder = s"$dataFolder/${b.folder}/$kindOfData"
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
        .json(outputBlobFolder)
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

}
