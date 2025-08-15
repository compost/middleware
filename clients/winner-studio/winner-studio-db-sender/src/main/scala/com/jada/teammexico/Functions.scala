package com.jada.winnerstudio

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
import java.util.Optional
import com.microsoft.azure.functions.annotation.AuthorizationLevel

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

  /*
  brand-queue."136"=https://sqs.eu-central-1.amazonaws.com/663880797555/winnerstudio-bingo-raider-jada-eventhub.fifo
  brand-queue."137"=https://sqs.eu-central-1.amazonaws.com/663880797555/winnerstudio-bingo-raider-jada-eventhub.fifo
  mapping-selector-dev-prefix[0]=137

  brand-queue."138"=https://sqs.eu-central-1.amazonaws.com/663880797555/winnerstudio-cash-trip-jada-eventhub.fifo
  brand-queue."139"=https://sqs.eu-central-1.amazonaws.com/663880797555/winnerstudio-cash-trip-jada-eventhub.fifo
  mapping-selector-dev-prefix[1]=139

  brand-queue."140"=https://sqs.eu-central-1.amazonaws.com/663880797555/winnerstudio-jada-eventhub.fifo
  brand-queue."141"=https://sqs.eu-central-1.amazonaws.com/663880797555/winnerstudio-jada-eventhub.fifo
  mapping-selector-dev-prefix[2]=141
   */
  val bingo = new Brand(
    id = 136,
    folder = "bingo",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/winnerstudio-bingo-raider-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val cash = new Brand(
    id = 138,
    folder = "cash",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/winnerstudio-cash-trip-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val main = new Brand(
    id = 140,
    folder = "main-winner",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/winnerstudio-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )

  lazy val spark = SparkSession.builder
    .master("local[*]")
    .appName("WinnerStudioGetBatchUpdate")
    .config("spark.driver.memory", "1G")
    .getOrCreate()

  val dryRun = Option(System.getenv("DRY_RUN")).map(_.toBoolean).getOrElse(true)
  val storageName = "centraliseddwa6a1"
  val storageAccessKey = System.getenv("STORAGE_ACCESS_KEY")
  val outputContainerName = "winnerstudio"
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

    runDC(bingo)
    runDC(cash)
    runDC(main)
  }

  def runDC(b: Brand)(implicit logger: Logger) = {
    val query =
      s"select player_id as id, num_deposits as lifetime_deposit_count, deposits as lifetime_deposit_amount FROM bus.V_PLAYERS_LIFETIME_VALUES WHERE BRAND_ID ${b.filter} AND DATEDIFF(day, LAST_UPDATE, GETDATE()) <= 7"

    val df = spark.read
      .format(net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME)
      .option("query", query)
      .options(sfOptions)
      .load()

    import functions._

    import spark.implicits._
    val newDf = df
      .withColumn(
        "lifetime_deposit_count",
        $"lifetime_deposit_count".cast(IntegerType).cast(StringType)
      )
      .withColumn(
        "lifetime_deposit_amount",
        round($"lifetime_deposit_amount", 2)
      )
      .select(
        df.col("id"),
        map(
          lit("lifetime_deposit_count"),
          $"lifetime_deposit_count",
          lit("lifetime_deposit_amount"),
          $"lifetime_deposit_amount"
        )
          .as("properties")
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
    runLastAD(bingo)
    runLastAD(cash)
    runLastAD(main)
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
    runLastBD(bingo)
    runLastBD(cash)
    runLastBD(main)
  }

  def runLastBD(b: Brand)(implicit logger: Logger) = {
    val query =
      s"""
         |SELECT player_id  as id, TO_VARCHAR (max(bet_datetime), 'yyyy-MM-dd') as last_bet_date
         |from dbo.fct_wagering w
         |INNER JOIN dbo.dim_players p on w.player_wid = p.player_wid AND p.brand_id = w.brand_id
         |where w.brand_id ${b.filter}
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
    runLastDepD(bingo)
    runLastDepD(cash)
    runLastDepD(main)
  }

  def runLastDepD(b: Brand)(implicit logger: Logger) = {
    val query =
      s"""
         |SELECT player_id as id, TO_VARCHAR (max(transaction_datetime), 'yyyy-MM-dd') as last_deposit_date
         |from dbo.fct_wallet w
         |INNER JOIN dbo.dim_players p on w.player_wid = p.player_wid AND p.brand_id = w.brand_id
         |inner join dbo.dim_transaction_subtypes st on st.transaction_subtype_id = w.transaction_subtype_id and st.brand_id = w.brand_id
         | WHERE st.transaction_type_id = ${Deposit} and w.brand_id ${b.filter}
         | AND DATEDIFF(day, TRY_TO_DATE(TO_CHAR(w.pif_delta),'YYYYMMDDHHMISS'), GETDATE()) <= 8
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
      "last_deposit_date",
      "last_deposit_date.json"
    )
  }
  @FunctionName("FirstDepositDatetime")
  def runFirstDepositDatetime(
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
    runFirstDepositDatetimeSub(bingo)
    runFirstDepositDatetimeSub(cash)
    runFirstDepositDatetimeSub(main)
  }

  def runFirstDepositDatetimeSub(b: Brand)(implicit logger: Logger) = {
    val query =
      s"""
         |SELECT player_id as id, TO_VARCHAR (first_dep_datetime, 'yyyy-MM-dd') as first_dep_datetime
         |FROM dbo.dim_players 
         |WHERE brand_id ${b.filter} and first_dep_datetime is not null
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
        lit("first_dep_datetime"),
        $"first_dep_datetime"
      ).as("properties")
    )

    saveDFAndSendBatchNotif(
      newDf,
      b,
      "first_dep_datetime",
      "first_dep_datetime.json"
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
         |  "mappingSelector": "batch_update",
         |  "downloadUri": "$downloadUrl"
         |}
         |""".stripMargin

    val request = new SendMessageRequest()
      .withQueueUrl(queue)
      .withMessageBody(body)
      .withMessageGroupId("winner-studio")
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
