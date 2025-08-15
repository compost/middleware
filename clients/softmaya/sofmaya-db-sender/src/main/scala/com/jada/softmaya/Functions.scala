package com.jada.softmaya

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

  val original = new Brands(
    ids = Set(73),
    folder = "original",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/softmays-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val pokies = new Brands(
    ids = Set(72),
    folder = "pokies",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/softmaya-thepokies-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )
  val betshift = new Brands(
    ids = Set(164),
    folder = "betshift",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/softmaya-2-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val Deposit = 1
  lazy val spark = SparkSession.builder
    .master("local[*]")
    .appName("softmayaGetBatchUpdate")
    .config("spark.driver.memory", "1G")
    .getOrCreate()
  val connectionUrl = System.getenv("CONNECTION_URL")
  val dryRun = Option(System.getenv("DRY_RUN")).exists(_.toBoolean)
  val storageName = "centraliseddwa6a1"
  val storageAccessKey = System.getenv("STORAGE_ACCESS_KEY")
  val outputContainerName = "softmaya"
  spark.conf.set(
    s"fs.azure.account.key.$storageName.blob.core.windows.net",
    storageAccessKey
  )
  val connectionStringBlob =
    s"DefaultEndpointsProtocol=https;AccountName=$storageName;AccountKey=$storageAccessKey;EndpointSuffix=core.windows.net"

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
    "pem_private_key" -> PrivateKey.getPem(),
    "sfURL" -> getenv("SF_URL", "irzzdfb-bj65063.snowflakecomputing.com"),
    "sfUser" -> getenv("SF_USER", "APP_MIDDLEWARE_SERVICE"),
    "sfDatabase" -> getenv("SF_DATABASE", "CENTRALDW"),
    "sfSchema" -> getenv("SF_SCHEMA", "dbo"),
    "sfWarehouse" -> getenv("SF_WAREHOUSE", "DELIVERY_WH")
  )

  @FunctionName("LastBetDatetime")
  def runLastBetDatetime(
      @TimerTrigger(
        name = "LastBetDatetime",
        schedule = "0 20 5 * * *"
      ) timerInfo: String,
      context: ExecutionContext
  ): Unit = {
    implicit val logger = context.getLogger
    logger.info(s"starting app $timerInfo")
    runLastBetDatetimeSub(original)
    runLastBetDatetimeSub(betshift)
  }

  def runLastBetDatetimeSub(
      b: Brands
  )(implicit logger: java.util.logging.Logger): Unit = {
    val query =
      s"""
         |SELECT player_id  as id, TO_VARCHAR (max(bet_datetime), 'yyyy-MM-dd') as last_bet_date
         |from dbo.fct_wagering w
         |INNER JOIN dbo.dim_players p on w.player_wid = p.player_wid AND p.brand_id = w.brand_id
         |where w.brand_id ${b.filter} AND DATEDIFF(day, TRY_TO_DATE(TO_CHAR(w.pif_delta),'YYYYMMDDHHMISS'), GETDATE()) <= 8
         |group by player_id
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
      "last_bet_date"
    )
  }

  @FunctionName("LastDepositDatetime")
  def runLastDepositDatetime(
      @TimerTrigger(
        name = "LastDepositDatetime",
        schedule = "0 25 5 * * *"
      ) timerInfo: String,
      context: ExecutionContext
  ): Unit = {
    implicit val logger = context.getLogger
    logger.info(s"starting app $timerInfo")
    runLastDepositDatetimeSub(original)
    runLastDepositDatetimeSub(betshift)
  }
  def runLastDepositDatetimeSub(
      b: Brands
  )(implicit logger: java.util.logging.Logger): Unit = {
    val query =
      s"""
         |SELECT player_id as id,  TO_VARCHAR (max(transaction_datetime), 'yyyy-MM-dd') as last_deposit_date
         |from dbo.fct_wallet w
         |INNER JOIN dbo.dim_players p on w.player_wid = p.player_wid AND p.brand_id = w.brand_id
         |inner join dbo.dim_transaction_subtypes st on st.transaction_subtype_id = w.transaction_subtype_id and st.brand_id = w.brand_id
         |where w.brand_id ${b.filter} AND transaction_type_id = 1 AND w.transaction_status_id = 1
         |AND DATEDIFF(day, TRY_TO_DATE(TO_CHAR(w.pif_delta),'YYYYMMDDHHMISS'), GETDATE()) <= 8
         |group by player_id
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
        lit("last_deposit_date"),
        $"last_deposit_date"
      ).as("properties")
    )

    saveDFAndSendBatchNotif(
      newDf,
      b,
      "last_deposit_date"
    )
  }

  @FunctionName("GetDepositCount")
  def runGetDepositCount(
      @TimerTrigger(
        name = "GetDepositCount",
        schedule = "0 35 5 * * *"
      ) timerInfo: String,
      context: ExecutionContext
  ): Unit = {
    implicit val logger = context.getLogger
    logger.info(s"starting app $timerInfo")
    runGetDepositCountSub(original)
    runGetDepositCountSub(betshift)
  }

  def runGetDepositCountSub(
      b: Brands
  )(implicit logger: java.util.logging.Logger): Unit = {
    val query =
      s"select player_id as id, num_deposits as lifetime_deposit_count, deposits as lifetime_deposit_amount FROM bus.V_PLAYERS_LIFETIME_VALUES WHERE BRAND_ID ${b.filter} AND DATEDIFF(day, LAST_UPDATE, GETDATE()) <= 7"

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
        lit("lifetime_deposit_count"),
        $"lifetime_deposit_count",
        lit("lifetime_deposit_amount"),
        $"lifetime_deposit_amount"
      ).as("properties")
    )

    saveDFAndSendBatchNotif(
      newDf,
      b,
      "lifetime_deposit"
    )
  }

  @FunctionName("LastActivityDate")
  def runLastActivityDate(
      @TimerTrigger(
        name = "LastActivityDate",
        schedule = "0 40 5 * * *"
      ) timerInfo: String,
      context: ExecutionContext
  ): Unit = {
    implicit val logger = context.getLogger
    logger.info(s"starting app $timerInfo")
    runLastActivityDateSub(original)
    runLastActivityDateSub(betshift)
  }

  def runLastActivityDateSub(
      b: Brands
  )(implicit logger: java.util.logging.Logger): Unit = {
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
      "last_activity_date"
    )
  }

  @FunctionName("GameRecommendations")
  def runGameRecommendations(
      @TimerTrigger(
        name = "GameRecommendations",
        schedule = "0 0 */2 * * *"
      ) timerInfo: String,
      context: ExecutionContext
  ): Unit = {
    implicit val logger = context.getLogger
    logger.info(s"starting app $timerInfo")
    runGameRecommendationsSub(original)
    runGameRecommendationsSub(betshift)
  }

  def runGameRecommendationsSub(
      b: Brands
  )(implicit logger: java.util.logging.Logger): Unit = {
    val query =
      s"""
    SELECT TO_JSON(OBJECT_CONSTRUCT('id', gr.player_id, 
    'properties', 
        OBJECT_CONSTRUCT(
        'recommendedgame1', gr.recommendedgame1,
        'recommendedgame2', gr.recommendedgame2,
        'recommendedgame3', gr.recommendedgame3,
        'recommendedgame4', gr.recommendedgame4,
        'recommendedgame5', gr.recommendedgame5
        ))) 
        AS value from bus.GAME_RECOMMENDATION gr 
        WHERE gr.brand_id ${b.filter}
      """.stripMargin

    logger.info(query)
    val df = spark.read
      .format(net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME)
      .option("query", query)
      .options(sfOptions)
      .load()

    saveDFAndSendBatchNotif(
      df,
      b,
      "game_recommendations",
      format = "txt"
    )
  }

  def saveDFAndSendBatchNotif(
      dataFrame: DataFrame,
      b: Brands,
      kindOfData: String,
      format: String = "json"
  )(implicit logger: Logger) = {
    import org.apache.hadoop.fs._

    val fileName = s"${kindOfData}.json"

    val outputContainerPath =
      s"wasbs://$outputContainerName@$storageName.blob.core.windows.net"
    val dataFolder = if (dryRun) "test" else "data"

    val destinationFolder = s"$dataFolder/${b.folder}/$kindOfData"
    val outputBlobFolder = s"$outputContainerPath/$destinationFolder"

    val df = dataFrame
      .coalesce(1)
      .write

    if (format == "json") {
      df.format("com.databricks.spark.csv")
        .option("header", "false")
        .mode("overwrite")
        .option("quote", " ")
        .option("escapeQuotes", "false")
        .option("escape", " ")
        .json(outputBlobFolder)
    } else {
      df.format("text").mode("overwrite").save(outputBlobFolder)
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
    b.sendNotification(dryRun, url)
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

  def sendNotification(dryRun: Boolean, downloadUrl: String)(implicit
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
      .withMessageGroupId("softmaya")
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
