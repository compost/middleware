package com.jada.playbook

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

  val bresbet = new Brand(
    id = 145,
    folder = "bresbet",
    mappingSelector = "batch-import",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/bresbet-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val devBresbet = new Brand(
    id = 146,
    folder = "bresbet_DEV",
    mappingSelector = "DEV_batch-import",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/bresbet-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )
  val star = new Brand(
    id = 166,
    folder = "star",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/star-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val devStar = new Brand(
    id = 167,
    folder = "star_DEV",
    mappingSelector = "DEV_batch-import",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/star-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )
  val rhino = new Brand(
    id = 170,
    folder = "rhino",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/rhino-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )
  val devRhino = new Brand(
    id = 171,
    folder = "rhino_dev",
    mappingSelector = "DEV_batch-import",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/rhino-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )
  val planetsportbet = new Brand(
    id = 174,
    folder = "planetsportbet",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/planetsportbet-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val devPlanetsportbet = new Brand(
    id = 175,
    folder = "planetsportbet_dev",
    mappingSelector = "DEV_batch-import",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/planetsportbet-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val betzone = new Brand(
    id = 168,
    folder = "betzone",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/betzone-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val devBetzone = new Brand(
    id = 169,
    mappingSelector = "DEV_batch-import",
    folder = "betzone_dev",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/betzone-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val akbet = new Brand(
    id = 205,
    folder = "akbet",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/akbet-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val devAkbets = new Brand(
    id = 206,
    folder = "akbet_dev",
    mappingSelector = "DEV_batch-import",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/akbet-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val dragonbet = new Brand(
    id = 207,
    folder = "dragonbet",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/dragonbet-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val devDragonbet = new Brand(
    id = 208,
    folder = "dragonbet_dev",
    mappingSelector = "DEV_batch-import",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/dragonbet-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val gentlemanjim = new Brand(
    id = 209,
    folder = "gentlemanjim",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/playbook-engineering-gentleman-jim-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val energy = new Brand(
    id = 211,
    folder = "energy",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/playbook-engineering-energy-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val pricedup = new Brand(
    id = 238,
    folder = "pricedup",
    queue =
      "https://sqs.eu-north-1.amazonaws.com/663880797555/eventhub_three_5288_pricedup.fifo",
    region = Regions.EU_NORTH_1
  )

  val betwright = new Brand(
    id = 265,
    folder = "betwright",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/playbook-engineering-betwright-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val devBetwright = new Brand(
    id = 266,
    folder = "betwright_dev",
    mappingSelector = "DEV_batch-import",
    queue =
      "https://sqs.eu-central-1.amazonaws.com/663880797555/playbook-engineering-betwright-jada-eventhub.fifo",
    region = Regions.EU_CENTRAL_1
  )

  val all =
    Set(
      bresbet,
      star,
      rhino,
      planetsportbet,
      betzone,
      akbet,
      dragonbet,
      devBresbet,
      devStar,
      devRhino,
      devPlanetsportbet,
      devBetzone,
      devAkbets,
      devDragonbet,
      gentlemanjim,
      energy,
      pricedup,
      betwright,
      devBetwright
    )

  lazy val spark = SparkSession.builder
    .master("local[*]")
    .appName("PlaybookGetBatchUpdate")
    .config("spark.driver.memory", "1G")
    .getOrCreate()

  val storageName = "centraliseddwa6a1"
  val storageAccessKey = System.getenv("STORAGE_ACCESS_KEY")
  val outputContainerName = "playbook"
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
    logger.info(s"starting app $timerInfo")
    all.foreach(runLastAD(_))
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
    all.foreach(runLastBD(_))
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
    all.foreach(runLastDepD(_))
  }

  def runLastDepD(b: Brand)(implicit logger: Logger) = {
    val query =
      s"""
         |SELECT player_id as id, TO_VARCHAR (max(transaction_datetime), 'yyyy-MM-dd') as last_deposit_date
         |from dbo.fct_wallet w
         |INNER JOIN dbo.dim_players p on w.player_wid = p.player_wid AND p.brand_id = w.brand_id
         |inner join dbo.dim_transaction_subtypes st on st.transaction_subtype_id = w.transaction_subtype_id and st.brand_id = w.brand_id
         | INNER JOIN dbo.dim_transaction_types dt ON dt.BRAND_ID = w.BRAND_ID AND dt.TRANSACTION_TYPE_ID = st.TRANSACTION_TYPE_ID
         | WHERE dt.transaction_type_id = ${Deposit} AND w.transaction_status_id = 2 and w.brand_id ${b.filter}
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

  @FunctionName("historyStakeFactorCI-1452")
  def runHistoryStakeFactor(
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
    Set(betwright, devBetwright).foreach(
      runHistoryStakeFactorSub(_)
    )
  }

  def runHistoryStakeFactorSub(b: Brand)(implicit logger: Logger) = {
    val query =
      s"""
         | SELECT player_id as id, 
         | NVL(inplay,'') as sf_inplay, 
         | NVL(inplay_casino,'') as sf_inplay_casino, 
         | NVL(inplay_livecasino,'') as sf_inplay_livecasino, 
         | NVL(inplay_lottery,'') as sf_inplay_lottery, 
         | NVL(inplay_virtualsports,'') as sf_inplay_virtualsports, 
         | NVL(prematch,'') as sf_prematch, 
         | NVL(prematch_casino,'') as sf_prematch_casino, 
         | NVL(prematch_livecasino,'') as sf_prematch_livecasino, 
         | NVL(prematch_lottery,'') as sf_prematch_lottery, 
         | NVL(prematch_virtualsports,'') as sf_prematch_virtualsports 
         | FROM BUS.RAW_CRM_IMPORT_STAKE_FACTOR
         | WHERE brand_id ${b.filter}
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
        lit("sf_inplay"),
        $"sf_inplay",
        lit("sf_inplay_casino"),
        $"sf_inplay_casino",
        lit("sf_inplay_livecasino"),
        $"sf_inplay_livecasino",
        lit("sf_inplay_lottery"),
        $"sf_inplay_lottery",
        lit("sf_inplay_virtualsports"),
        $"sf_inplay_virtualsports",
        lit("sf_prematch"),
        $"sf_prematch",
        lit("sf_prematch_casino"),
        $"sf_prematch_casino",
        lit("sf_prematch_livecasino"),
        $"sf_prematch_livecasino",
        lit("sf_prematch_lottery"),
        $"sf_prematch_lottery",
        lit("sf_prematch_virtualsports"),
        $"sf_prematch_virtualsports"
      ).as("properties")
    )

    saveDFAndSendBatchNotif(
      newDf,
      b,
      "history_stake_factor",
      "history_stake_factor.json"
    )
  }

  @FunctionName("CI-1452")
  def ci1452(
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

    val table = "BUS.RAW_CRM_IMPORT_PLAYBOOK"
    val properties = Map[String, String](
      "affiliate_id" -> "AFFILIATEID",
      "brand_id" -> "BRAND_ID",
      "country_description" -> "COUNTRY",
      "currency_description" -> "CURRENCY",
      "dob" -> "SUBSTR(DOB, 0, 10)",
      "email" -> "EMAIL",
      "first_dep_datetime" -> "SUBSTR(FIRST_DEP_DATE, 0, 10)",
      "first_name" -> "FIRST_NAME",
      "is_self_excluded" -> "IS_SELF_EXCLUDED",
      "last_name" -> "LAST_NAME",
      "email_consented" -> "iff(MARKETING_EMAIL like 'False', 'false', 'true')",
      "phone_consented" -> "iff(MARKETING_PHONE like 'False', 'false', 'true')",
      "sms_consented" -> "iff(MARKETING_SMS like 'False', 'false', 'true')",
      "phone_number" -> "PHONENUMBER",
      "player_id" -> "PLAYER_ID",
      "reg_datetime" -> "SUBSTR(REG_DATETIME, 0, 10)",
      "status" -> "STATUS",
      "test_user" -> "TEST_USER",
      "VIP" -> "VIP",
      "postcode" -> "POSTCODE",
      "promo_id" -> "PROMO_ID",
      "promo_banned" -> "PROMO_BANNED",
      "player_blocked_reason" -> "BLOCKED_REASON",
      "city" -> "CITY"
    ).map { case (k, v) => s"'$k', $v" }.mkString(",")
    val name = "history-player"
    Set(betwright, devBetwright).foreach(historySub(_, name, table, properties))
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
      format = "txt",
      send = true
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
      .withMessageGroupId("playbook")
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
