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

  val marsbet = new Brand(
    id = 242,
    folder = "marsbet",
    mappingSelector = "batch-import",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/marsbet-5285-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val devMarsbet = new Brand(
    id = 241,
    folder = "marsbet_DEV",
    mappingSelector = "DEV_batch-import",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/marsbet-5285-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val chockwin = new Brand(
    id = 295,
    folder = "chockwin",
    mappingSelector = "batch-import",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/eventhub_three_5307_Chokwin.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val devChockwin = new Brand(
    id = 294,
    folder = "chockwin_DEV",
    mappingSelector = "DEV_batch-import",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/eventhub_three_5307_Chokwin.fifo",
    region = Regions.EU_CENTRAL_1
  )

  lazy val spark = SparkSession.builder
    .master("local[*]")
    .appName("BaseGetBatchUpdate")
    .config("spark.driver.memory", "1G")
    .getOrCreate()

  val storageName = "centraliseddwa6a1"
  val kafkaBrokers = getenv("KAFKA_BROKERS", "127.0.0.1:9092")
  val storageAccessKey = System.getenv("STORAGE_ACCESS_KEY")
  val ids = System.getenv("BRAND_IDS").split(",").map(_.toInt)
  val all =
    Set(
      marsbet,
      devMarsbet
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
  @FunctionName("GetDepositCount")
  def runGetDepositCount(
      @TimerTrigger(
        name = "GetDepositCount",
        schedule = "0 0 10 * * *"
      ) timerInfo: String,
      context: ExecutionContext
  ): Unit = {
    implicit val logger = context.getLogger
    all.foreach(runDC(_))
  }

  def runDC(b: Brand)(implicit logger: Logger) = {
    val query =
      s"select player_id as id, num_deposits as lifetime_deposit_count, deposits as lifetime_deposit_amount FROM bus.V_PLAYERS_LIFETIME_VALUES WHERE BRAND_ID ${b.filter} AND DATEDIFF(day, LAST_UPDATE, GETDATE()) <= 7"
    val df = spark.read
      .format(net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME)
      .option("query", query)
      .options(sfOptions)
      .load()

    logger.info(s"$all $query")
    import spark.implicits._
    import functions._
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
    all.foreach(runLastAD(_))
  }
  def runLastAD(b: Brand)(implicit logger: Logger) = {
    val query =
      s"""
          |SELECT player_id as id, last_activity_date 
          |FROM CENTRALDW.BUS.PLAYERS_LAST_ACTIVITY_DATE 
          |WHERE brand_id ${b.filter} AND DATEDIFF(day, LAST_ACTIVITY_DATE, GETDATE()) <= 7
          |""".stripMargin

    logger.info(s"$all $query")
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
    all.foreach(runLastBD(_))
  }

  def runLastBD(b: Brand)(implicit logger: Logger) = {
    val query =
      s"""
         |SELECT player_id  as id, TO_VARCHAR (max(bet_datetime), 'YYYY-MM-DD') as last_bet_date
         |from dbo.fct_wagering w
         |INNER JOIN dbo.dim_players p on w.player_wid = p.player_wid AND p.brand_id = w.brand_id
         |where w.brand_id ${b.filter}
         |AND DATEDIFF(day, TRY_TO_DATE(TO_CHAR(w.pif_delta),'YYYYMMDDHHMISS'), GETDATE()) <= 8
         |group by player_id
      """.stripMargin

    logger.info(s"$all $query")
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
    all.foreach(runLastDepD(_))
  }

  def runLastDepD(b: Brand)(implicit logger: Logger) = {
    val query =
      s"""
         |SELECT player_id as id, TO_VARCHAR (max(transaction_datetime), 'YYYY-MM-DD') as last_deposit_date
         |from dbo.fct_wallet w
         |INNER JOIN dbo.dim_players p on w.player_wid = p.player_wid AND p.brand_id = w.brand_id
         |inner join dbo.dim_transaction_subtypes st on st.transaction_subtype_id = w.transaction_subtype_id and st.brand_id = w.brand_id
         | INNER JOIN dbo.dim_transaction_types dt ON dt.BRAND_ID = w.BRAND_ID AND dt.TRANSACTION_TYPE_ID = st.TRANSACTION_TYPE_ID
         | WHERE dt.transaction_type_id = ${Deposit} AND w.transaction_status_id = ${Complete} and w.brand_id ${b.filter}
         | AND DATEDIFF(day, TRY_TO_DATE(TO_CHAR(w.pif_delta),'YYYYMMDDHHMISS'), GETDATE()) <= 8
         |group by player_id
      """.stripMargin

    logger.info(s"$all $query")
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

  @FunctionName("history")
  def history(
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
      "player_id" -> "PLAYER_ID",
      "brand_id" -> "BRAND_ID",
      "affiliate_id" -> "AFFILIATE_ID",
      "country" -> "COUNTRY",
      "language" -> "LANGUAGE",
      "currency" -> "CURRENCY",
      "dob" -> "TO_VARCHAR(TO_DATE(dob, 'DD/MM/YYYY'), 'YYYY-MM-DD')",
      "email" -> "EMAIL",
      "first_dep_datetime" -> "SUBSTR(FIRST_DEP_DATETIME, 0, 10)",
      "first_name" -> "FIRST_NAME",
      "last_name" -> "LAST_NAME",
      "is_self_excluded" -> "IS_SELF_EXCLUDED",
      "phone_number" -> "PHONE_NUMBER",
      "reg_datetime" -> "SUBSTR(REG_DATETIME, 0, 10)",
      "test_user" -> "TEST_USER",
      "vip" -> "VIP"
    ).map { case (k, v) => s"'$k', $v" }.mkString(",")
    val name = "history-player"
    all.foreach(historySub(_, name, table, properties))
  }

  def historySub(
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

  @FunctionName("produceHistory")
  def produceHistory(
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
    runSub(
      "CENTRALDW.BUS.RAW_CRM_IMPORT_WISEGAMING",
      "history-players"
    )
  }
  def runSub(table: String, topic: String): Unit = {
    val df = spark.read
      .format(net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME)
      .option(
        "query",
        s"""
          |select brand_id || '-' || player_id as key, TO_JSON(OBJECT_CONSTRUCT(${table}.*)) as value
          |from ${table} where brand_id is not null and player_id is not null
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
