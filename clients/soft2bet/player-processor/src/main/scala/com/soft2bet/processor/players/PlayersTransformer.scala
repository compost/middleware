package com.soft2bet.processor.players

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.sqs.{AmazonSQS, AmazonSQSClientBuilder}
import com.amazonaws.services.sqs.model.SendMessageRequest
import com.soft2bet.Common
import com.soft2bet.model.{
  AccountFrozen,
  Achievement,
  DOBTempFix,
  Login,
  Player,
  PlayerStore,
  Verification,
  Wallet
}
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import org.jboss.logging.Logger
import io.circe.syntax._
import io.quarkiverse.loggingjson.providers.KeyValueStructuredArgument.kv

import scala.util.{Failure, Success, Try}
import com.soft2bet.model.PlayerStoreSQS
import com.soft2bet.model.Verification._
import com.jada.sqs.Body
import io.circe.Printer
import com.soft2bet.model.BonusUpdatedSQS
import com.soft2bet.model.Wagering
import com.soft2bet.model.WageringJSON
import com.soft2bet.model.keepYYYYMMDD
import com.soft2bet.model.formatYYYYMMDD
import com.soft2bet.model.WithdrawalRequestSQS
import java.util.UUID
import com.soft2bet.model.VerificationSQS

class PlayersTransformer(
    config: com.jada.configuration.ApplicationConfiguration,
    sqs: software.amazon.awssdk.services.sqs.SqsClient,
    ueNorthSQS: software.amazon.awssdk.services.sqs.SqsClient,
    playerStoreName: String
) extends Transformer[String, Array[Byte], KeyValue[String, PlayerStore]] {
  private var processorContext: ProcessorContext = _
  private var playerKVStore: KeyValueStore[String, PlayerStore] = _

  private val sender = new Sender(config, sqs, ueNorthSQS, false, false)
  final val printer: Printer = Printer(
    dropNullValues = true,
    indent = ""
  )
  val cryptos = Set(
    "letknowpay",
    "xbo",
    "coinpayments",
    "bitcoin",
    "doge",
    "litecoin",
    "bitcoin",
    "litecoin",
    "usdtether trc20",
    "usdtether erc20",
    "usdtether bep20",
    "ethereum",
    "ripple",
    "usdcoin",
    "bitcoin cash",
    "cardano"
  )
  private final val logger =
    Logger.getLogger(classOf[PlayersTransformer])

  override def init(processorContext: ProcessorContext): Unit = {
    this.processorContext = processorContext
    this.playerKVStore = getKVStore(playerStoreName)
  }

  def deserializePlayer(data: Array[Byte]): Player = {
    import io.circe._
    import io.circe.generic.auto._
    parser.decode[Player](new String(data)).right.get
  }

  def deserializeAchievement(data: Array[Byte]): Achievement = {
    import io.circe._
    import io.circe.generic.auto._
    parser.decode[Achievement](new String(data)).right.get
  }

  def deserializeAccountFrozen(data: Array[Byte]): AccountFrozen = {
    import io.circe._
    import io.circe.generic.auto._
    parser.decode[AccountFrozen](new String(data)).right.get
  }

  def deserializeVerification(data: Array[Byte]): Verification = {
    import io.circe._
    import io.circe.generic.auto._
    parser.decode[Verification](new String(data)).right.get
  }
  def deserializeWallet(data: Array[Byte]): Wallet = {
    import io.circe._
    import io.circe.generic.auto._
    parser.decode[Wallet](new String(data)).right.get
  }

  def deserializeWagering(data: Array[Byte]): Wagering = {
    import io.circe._
    import io.circe.generic.auto._
    parser.decode[Wagering](new String(data)).right.get
  }

  def deserializeLogin(data: Array[Byte]): Login = {
    import io.circe._
    import io.circe.generic.auto._
    parser.decode[Login](new String(data)).right.get
  }

  def deserializeDOB(data: Array[Byte]): DOBTempFix = {
    import io.circe._
    import io.circe.generic.auto._
    parser.decode[DOBTempFix](new String(data)).right.get
  }

  override def transform(
      k: String,
      v: Array[Byte]
  ): KeyValue[String, PlayerStore] = {
    val playerFromStore = playerKVStore.get(k)

    val topicName = processorContext.topic()
    val playerToSaveInStore = topicName match {
      case Common.playersRepartitionedTopic =>
        val inputPlayer = deserializePlayer(v)
        logger.debugv(
          "transform",
          Array(kv("key", k), kv("input", new String(v))): _*
        )
        val old = Option(playerFromStore)
        val newPlayer = old match {
          case Some(player) =>
            player.copy(
              player_id = Some(k),
              first_name = inputPlayer.first_name.orElse(player.first_name),
              RegistrationStatus = inputPlayer.RegistrationStatus.orElse(
                player.RegistrationStatus
              ),
              WelcomeChosenBonus = inputPlayer.WelcomeChosenBonus.orElse(
                player.WelcomeChosenBonus
              ),
              last_name = inputPlayer.last_name.orElse(player.last_name),
              email = inputPlayer.email.orElse(player.email),
              phone_number =
                inputPlayer.phone_number.orElse(player.phone_number),
              is_self_excluded =
                inputPlayer.is_self_excluded.orElse(player.is_self_excluded),
              vip = inputPlayer.vip.orElse(player.vip),
              reg_datetime =
                inputPlayer.reg_datetime.orElse(player.reg_datetime),
              language = inputPlayer.language.orElse(player.language),
              affiliate_id =
                inputPlayer.affiliate_id.orElse(player.affiliate_id),
              dob = inputPlayer.dob.orElse(player.dob),
              country_id = inputPlayer.country_id.orElse(player.country_id),
              is_suppressed =
                inputPlayer.is_suppressed.orElse(player.is_suppressed),
              brand_id = inputPlayer.brand_id.orElse(player.brand_id),
              brand_name = PlayerStoreSQS.fixBrandName(
                inputPlayer.brand_id.orElse(player.brand_id),
                inputPlayer.brand_name.orElse(player.brand_name)
              ),
              Postcode = inputPlayer.Postcode.orElse(player.Postcode),
              Country = inputPlayer.Country.orElse(player.Country),
              IsSelfExcludedOtherBrand = inputPlayer.IsSelfExcludedOtherBrand
                .orElse(player.IsSelfExcludedOtherBrand),
              AccountType = inputPlayer.AccountType.orElse(player.AccountType),
              BonusProgram =
                inputPlayer.BonusProgram.orElse(player.BonusProgram),
              Currency = inputPlayer.Currency.orElse(player.Currency),
              Duplicate = inputPlayer.Duplicate.orElse(player.Duplicate),
              GDPR = inputPlayer.GDPR.orElse(player.GDPR),
              Gender = inputPlayer.Gender.orElse(player.Gender),
              ReferralType =
                inputPlayer.ReferralType.orElse(player.ReferralType),
              RegistrationPlatform = inputPlayer.RegistrationPlatform
                .orElse(player.RegistrationPlatform),
              Username = inputPlayer.Username.orElse(player.Username),
              IsEmailValid =
                inputPlayer.IsEmailValid.orElse(player.IsEmailValid),
              IsVerified = inputPlayer.IsVerified.orElse(player.IsVerified),
              Avatar = inputPlayer.Avatar.orElse(player.Avatar),
              BonusChosen = inputPlayer.BonusChosen.orElse(player.BonusChosen),
              ZeroBounce = inputPlayer.ZeroBounce.orElse(player.ZeroBounce),
              License = inputPlayer.License.orElse(player.License),
              isZeroBalance =
                inputPlayer.isZeroBalance.orElse(player.isZeroBalance),
              GlobalProductClassification =
                inputPlayer.GlobalProductClassification
                  .orElse(player.GlobalProductClassification),
              ProductClassification = inputPlayer.ProductClassification
                .orElse(player.ProductClassification),
              FavoriteGameIds = player.FavoriteGameIds,
              FavoriteGameNames = player.FavoriteGameNames,
              BonusAbusing =
                inputPlayer.BonusAbusing.orElse(player.BonusAbusing),
              BlockedCasino =
                inputPlayer.BlockedCasino.orElse(player.BlockedCasino),
              BlockedSport =
                inputPlayer.BlockedSport.orElse(player.BlockedSport),
              CanBeContactedBySMS = inputPlayer.CanBeContactedBySMS.orElse(
                player.CanBeContactedBySMS
              ),
              CanBeContactedByEmail = inputPlayer.CanBeContactedByEmail
                .orElse(player.CanBeContactedByEmail),
              CanBeContactedByPhoneOrDynamicMessage =
                inputPlayer.CanBeContactedByPhoneOrDynamicMessage.orElse(
                  player.CanBeContactedByPhoneOrDynamicMessage
                ),
              CanReceivePromotions = inputPlayer.CanReceivePromotions.orElse(
                player.CanReceivePromotions
              ),
              SportPreference =
                inputPlayer.SportPreference.orElse(player.SportPreference),
              TotalBetsAmountSport = inputPlayer.TotalBetsAmountSport.orElse(
                player.TotalBetsAmountSport
              ),
              TotalBetsCountSport = inputPlayer.TotalBetsCountSport.orElse(
                player.TotalBetsCountSport
              ),
              AverageBetSizeSport = inputPlayer.AverageBetSizeSport.orElse(
                player.AverageBetSizeSport
              ),
              BonusToDepositRatioPercentage =
                inputPlayer.BonusToDepositRatioPercentage.orElse(
                  player.BonusToDepositRatioPercentage
                ),
              TurnoverRatioCasino = inputPlayer.TurnoverRatioCasino.orElse(
                player.TurnoverRatioCasino
              ),
              TurnoverRatioSport = inputPlayer.TurnoverRatioSport.orElse(
                player.TurnoverRatioSport
              ),
              ExternalAffiliateID = inputPlayer.ExternalAffiliateID.orElse(
                player.ExternalAffiliateID
              ),
              IsOptInCashback =
                inputPlayer.IsOptInCashback.orElse(player.IsOptInCashback),
              IsOptInWeeklyReload = inputPlayer.IsOptInWeeklyReload.orElse(
                player.IsOptInWeeklyReload
              ),
              IsOptInWeekendReload = inputPlayer.IsOptInWeekendReload
                .orElse(player.IsOptInWeekendReload),
              IsOptInForLastFourPeriodsCashback =
                inputPlayer.IsOptInForLastFourPeriodsCashback.orElse(
                  player.IsOptInForLastFourPeriodsCashback
                ),
              IsOptInForLastFourPeriodsWeeklyReload =
                inputPlayer.IsOptInForLastFourPeriodsWeeklyReload.orElse(
                  player.IsOptInForLastFourPeriodsWeeklyReload
                ),
              IsOptInForLastFourPeriodsWeekendReload =
                inputPlayer.IsOptInForLastFourPeriodsWeekendReload.orElse(
                  player.IsOptInForLastFourPeriodsWeekendReload
                ),
              registrationSource = inputPlayer.registrationSource.orElse(
                player.registrationSource
              ),
              VariantExperimentVersion =
                inputPlayer.VariantExperimentVersion.orElse(
                  player.VariantExperimentVersion
                ),
              wos = inputPlayer.wos.orElse(
                player.wos
              ),
              wos2 = inputPlayer.wos2.orElse(
                player.wos2
              )
            )

          case None =>
            // partially register non null infos
            PlayerStore(
              player_id = Some(k),
              first_name = inputPlayer.first_name,
              WelcomeChosenBonus = inputPlayer.WelcomeChosenBonus,
              RegistrationStatus = inputPlayer.RegistrationStatus,
              last_name = inputPlayer.last_name,
              email = inputPlayer.email,
              phone_number = inputPlayer.phone_number,
              is_self_excluded = inputPlayer.is_self_excluded,
              vip = inputPlayer.vip,
              reg_datetime = inputPlayer.reg_datetime,
              language = inputPlayer.language,
              affiliate_id = inputPlayer.affiliate_id,
              dob = inputPlayer.dob,
              country_id = inputPlayer.country_id,
              is_suppressed = inputPlayer.is_suppressed,
              brand_id = inputPlayer.brand_id,
              brand_name = PlayerStoreSQS
                .fixBrandName(inputPlayer.brand_id, inputPlayer.brand_name),
              Postcode = inputPlayer.Postcode,
              Country = inputPlayer.Country,
              IsSelfExcludedOtherBrand = inputPlayer.IsSelfExcludedOtherBrand,
              AccountType = inputPlayer.AccountType,
              BonusProgram = inputPlayer.BonusProgram,
              Currency = inputPlayer.Currency,
              Duplicate = inputPlayer.Duplicate,
              GDPR = inputPlayer.GDPR,
              Gender = inputPlayer.Gender,
              ReferralType = inputPlayer.ReferralType,
              RegistrationPlatform = inputPlayer.RegistrationPlatform,
              Username = inputPlayer.Username,
              IsEmailValid = inputPlayer.IsEmailValid,
              IsVerified = inputPlayer.IsVerified,
              Avatar = inputPlayer.Avatar,
              BonusChosen = inputPlayer.BonusChosen,
              ZeroBounce = inputPlayer.ZeroBounce,
              License = inputPlayer.License,
              isZeroBalance = inputPlayer.isZeroBalance,
              GlobalProductClassification =
                inputPlayer.GlobalProductClassification,
              ProductClassification = inputPlayer.ProductClassification,
              BonusAbusing = inputPlayer.BonusAbusing,
              BlockedCasino = inputPlayer.BlockedCasino,
              BlockedSport = inputPlayer.BlockedSport,
              CanBeContactedBySMS = inputPlayer.CanBeContactedBySMS,
              CanBeContactedByEmail = inputPlayer.CanBeContactedByEmail,
              CanBeContactedByPhoneOrDynamicMessage =
                inputPlayer.CanBeContactedByPhoneOrDynamicMessage,
              CanReceivePromotions = inputPlayer.CanReceivePromotions,
              ExternalAffiliateID = inputPlayer.ExternalAffiliateID,
              SportPreference = inputPlayer.SportPreference,
              TotalBetsAmountSport = inputPlayer.TotalBetsAmountSport,
              TotalBetsCountSport = inputPlayer.TotalBetsCountSport,
              AverageBetSizeSport = inputPlayer.AverageBetSizeSport,
              BonusToDepositRatioPercentage =
                inputPlayer.BonusToDepositRatioPercentage,
              TurnoverRatioCasino = inputPlayer.TurnoverRatioCasino,
              TurnoverRatioSport = inputPlayer.TurnoverRatioSport,
              IsOptInCashback = inputPlayer.IsOptInCashback,
              IsOptInWeeklyReload = inputPlayer.IsOptInWeeklyReload,
              IsOptInWeekendReload = inputPlayer.IsOptInWeekendReload,
              IsOptInForLastFourPeriodsCashback =
                inputPlayer.IsOptInForLastFourPeriodsCashback,
              IsOptInForLastFourPeriodsWeeklyReload =
                inputPlayer.IsOptInForLastFourPeriodsWeeklyReload,
              IsOptInForLastFourPeriodsWeekendReload =
                inputPlayer.IsOptInForLastFourPeriodsWeekendReload,
              registrationSource = inputPlayer.registrationSource,
              VariantExperimentVersion = inputPlayer.VariantExperimentVersion,
              wos = inputPlayer.wos,
              wos2 = inputPlayer.wos2
            )
        }
        newPlayer
      case Common.accountfrozenRepartitionedTopic =>
        val inputAccountFrozen = deserializeAccountFrozen(v)
        Option(playerFromStore) match {
          case Some(player) =>
            player.copy(
              player_id = Some(k),
              brand_id = inputAccountFrozen.brand_id.orElse(player.brand_id),
              block_reason =
                inputAccountFrozen.BlockReason.orElse(player.block_reason),
              is_blocked =
                inputAccountFrozen.IsBlocked.orElse(player.is_blocked),
              BlockedCasino =
                inputAccountFrozen.BlockedCasino.orElse(player.BlockedCasino),
              BlockedSport =
                inputAccountFrozen.BlockedSport.orElse(player.BlockedSport)
            )
          case _ =>
            PlayerStore(
              player_id = Some(k),
              brand_id = inputAccountFrozen.brand_id,
              block_reason = inputAccountFrozen.BlockReason,
              is_blocked = inputAccountFrozen.IsBlocked,
              BlockedCasino = inputAccountFrozen.BlockedCasino,
              BlockedSport = inputAccountFrozen.BlockedSport
            )
        }
      case Common.verificationRepartitionedTopic =>
        val verification = deserializeVerification(v)

        val ecr = if (verification.IsEmailConfirmed.contains("true")) {
          verification.registrationSource
        } else {
          None
        }
        val pcr = if (verification.IsPhoneConfirmed.contains("true")) {
          verification.registrationSource
        } else {
          None
        }
        val update = Option(playerFromStore) match {
          case Some(player) =>
            player.copy(
              brand_id = verification.brand_id.orElse(player.brand_id),
              IsVerified = verification.IsVerified.orElse(player.IsVerified),
              IsVerifiedFromVerification = verification.IsVerified
                .map(_.toLowerCase)
                .filter(v => v == "true")
                .orElse(
                  player.IsVerifiedFromVerification
                ),
              IsEmailConfirmed = verification.IsEmailConfirmed
                .orElse(
                  player.IsEmailConfirmed
                ),
              IsPhoneConfirmed = verification.IsPhoneConfirmed
                .orElse(
                  player.IsPhoneConfirmed
                ),
              PhoneConfirmedRegistrationSource = pcr
                .orElse(player.PhoneConfirmedRegistrationSource),
              EmailConfirmedRegistrationSource = ecr
                .orElse(player.EmailConfirmedRegistrationSource),
              VerificationStatus = verification.VerificationStatus.orElse(
                player.VerificationStatus
              )
            )
          case _ =>
            PlayerStore(
              player_id = verification.player_id,
              brand_id = verification.brand_id,
              IsVerified = verification.IsVerified,
              IsVerifiedFromVerification = verification.IsVerified
                .map(_.toLowerCase)
                .filter(v => v == "true"),
              IsEmailConfirmed = verification.IsEmailConfirmed,
              IsPhoneConfirmed = verification.IsPhoneConfirmed,
              PhoneConfirmedRegistrationSource = pcr,
              EmailConfirmedRegistrationSource = ecr,
              VerificationStatus = verification.VerificationStatus
            )
        }

        sendIsVerified(playerFromStore, update, verification)
        update

      case Common.achievementRepartitionedTopic =>
        // we send only last achievement that was completed
        val inputAchievement = deserializeAchievement(v)
        if (inputAchievement.AchievementCompleted.getOrElse("") == "true") {
          Option(playerFromStore) match {
            case Some(player) =>
              player.copy(
                player_id = Some(k),
                achievement_name = inputAchievement.AchievementName,
                achievement_completed = inputAchievement.AchievementCompleted
              )
            case _ =>
              PlayerStore(
                player_id = Some(k),
                achievement_name = inputAchievement.AchievementName,
                achievement_completed = inputAchievement.AchievementCompleted
              )
          }
        } else null
      case Common.walletRepartitionedTopic =>
        val wallet = deserializeWallet(v)

        if (
          wallet.brand_id.isDefined &&
          wallet.transaction_type_id.isDefined &&
          wallet.transaction_type_id.get.toLowerCase.startsWith("withdraw") &&
          wallet.transaction_status_id.isDefined &&
          wallet.transaction_status_id.get.toLowerCase.equals("pending") &&
          wallet.amount.isDefined
        ) {
          val body = Body[WithdrawalRequestSQS](
            "GENERIC_USER",
            wallet.player_id.get,
            "withdrawal_request",
            WithdrawalRequestSQS(wallet)
          )
          sender.sendToSQS(
            printer.print(Body.bodyEncoder[WithdrawalRequestSQS].apply(body)),
            wallet.player_id.get,
            wallet.brand_id.get
          )

        }
        // {"transaction_id":"QWIN_311836342","brand_id":"81","transaction_type_id":"withdraw-pgw_iq_bankintl","transaction_status_id":"cancel","amount":"70.00","last_de
        // posit_datetime":null,"first_deposit_datetime":null,"transaction_datetime":"2023-10-30 14:09:39","player_id":"QWIN_36801114","currency_id":null,"CasinoBonusBal
        // ance":null,"CasinoBonusBalanceEUR":null,"SportBonusBalance":null,"SportBonusBalanceEUR":null,"TotalWithdrawalLifetime":null,"TotalWithdrawalLifetimeEUR":null,
        // "LifetimeWithdrawalCount":null,"LifetimeDepositCount":null}
        if (
          wallet.brand_id.isDefined &&
          wallet.transaction_type_id.isDefined &&
          wallet.transaction_type_id.get.toLowerCase.startsWith("withdraw") &&
          wallet.transaction_status_id.isDefined &&
          wallet.transaction_status_id.get.toLowerCase.startsWith("cancel") &&
          wallet.amount.isDefined
        ) {
          val body =
            s"""
                 |{"type": "GENERIC_USER",
                 |"mappingSelector": "withdrawal_cancel",
                 |"contactId": "${wallet.player_id.get}",
                 |"properties": {
                 | "lastamountwithdrawcancel": "${wallet.amount.get}"
                 |}
                 |}
                 |""".stripMargin

          sender.sendToSQS(
            body,
            k,
            wallet.brand_id.get
          )
        }

        if (
          false && (wallet.brand_id.isDefined &&
            // Question should be in class or not
            (wallet.CasinoBonusBalance.isDefined ||
              wallet.SportBonusBalance.isDefined ||
              wallet.CasinoBonusBalanceEUR.isDefined ||
              wallet.SportBonusBalanceEUR.isDefined ||
              wallet.LifetimeWithdrawalCount.isDefined ||
              wallet.TotalWithdrawalLifetime.isDefined ||
              wallet.TotalWithdrawalLifetimeEUR.isDefined))
        ) {
          val body = Body[BonusUpdatedSQS](
            "GENERIC_USER",
            k,
            "bonusUpdated",
            BonusUpdatedSQS(wallet)
          )
          sender.sendToSQS(
            printer.print(Body.bodyEncoder[BonusUpdatedSQS].apply(body)),
            k,
            wallet.brand_id.get
          )
        }
        if (
          wallet.transaction_type_id
            .map(_.toLowerCase())
            .map(_.contains("deposit"))
            .getOrElse(false) && wallet.transaction_status_id == Some(
            "complete"
          )
          && wallet.PayMethod
            .map(_.toLowerCase())
            .exists(method => cryptos.exists(method.contains))
        ) {
          if (wallet.brand_id.isDefined) {
            val attributesList = List(
              wallet.player_id
                .map(x => s""""originalId": "$x"""")
                .getOrElse(""),
              wallet.brand_id
                .map(x => s""""brandId": "$x"""")
                .getOrElse(""),
              wallet.transaction_datetime
                .map(keepYYYYMMDD(_))
                .map(x => s""""Last_Crypto_dep_date": "$x"""")
                .getOrElse(""),
              wallet.amount
                .map(keepYYYYMMDD(_))
                .map(x => s""""Last_Dep_Crypto_Amount": "$x"""")
                .getOrElse("")
            ).filter(x => x != "")

            val body =
              s"""
               |{"type": "GENERIC_USER",
               |"mappingSelector": "payMethod_Crypto_Used",
               |"contactId": "$k",
               |"properties": {
               | ${attributesList.mkString(",")}
               |}
               |}
               |""".stripMargin

            sender.sendToSQS(
              body,
              k,
              wallet.brand_id.get
            )
          } else {
            logger.debug(s"${wallet.player_id} brand undefined")
          }
        }
        if (
          wallet.LifetimeDepositCount == Some(
            "1"
          ) && wallet.transaction_status_id == Some("complete")
        ) {
          if (wallet.brand_id.isDefined) {
            val attributesList = List(
              wallet.player_id
                .map(x => s""""playerID": "$x"""")
                .getOrElse(""),
              wallet.transaction_status_id
                .map(x => s""""TransactionStatus": "$x"""")
                .getOrElse(""),
              wallet.LifetimeDepositCount
                .map(x => s""""LifetimeDepositCount": "$x"""")
                .getOrElse("")
            ).filter(x => x != "")

            val body =
              s"""
                   |{"type": "GENERIC_USER",
                   |"mappingSelector": "first_deposit_completed",
                   |"contactId": "$k",
                   |"properties": {
                   | ${attributesList.mkString(",")}
                   |}
                   |}
                   |""".stripMargin

            sender.sendToSQS(
              body,
              k,
              wallet.brand_id.get
            )
          } else {
            logger.debug(s"${wallet.player_id} brand undefined")
          }
        } else {
          logger.debug(s"${wallet} not sent")
        }

        Option(playerFromStore) match {
          case Some(player) =>
            player.copy(
              player_id = wallet.player_id.orElse(player.player_id),
              brand_id = wallet.brand_id.orElse(player.brand_id),
              first_dep_datetime = wallet.first_deposit_datetime
                .orElse(player.first_dep_datetime),
              failed_deposit_date =
                (if (wallet.transaction_status_id.getOrElse("") == "decline")
                   wallet.transaction_datetime
                 else None).orElse(player.failed_deposit_date),
              is_first_deposit =
                (if (
                   (wallet.last_deposit_datetime.isEmpty && wallet.first_deposit_datetime.isDefined)
                   || (wallet.last_deposit_datetime.isDefined && wallet.first_deposit_datetime == wallet.last_deposit_datetime)
                 )
                   Some(true)
                 else None).orElse(player.is_first_deposit)
            )

          case _ =>
            PlayerStore(
              player_id = wallet.player_id,
              brand_id = wallet.brand_id,
              first_dep_datetime = wallet.first_deposit_datetime,
              failed_deposit_date =
                if (wallet.transaction_status_id.getOrElse("") == "decline")
                  wallet.transaction_datetime
                else None,
              is_first_deposit =
                if (
                  (wallet.last_deposit_datetime.isEmpty && wallet.first_deposit_datetime.isDefined)
                  || (wallet.last_deposit_datetime.isDefined && wallet.first_deposit_datetime == wallet.last_deposit_datetime)
                )
                  Some(true)
                else None
            )
        }
      case Common.loginRepartitionedTopic =>
        val login = deserializeLogin(v)
        if (login.login_success.getOrElse("") == "1") {
          val p = Option(playerFromStore) match {
            case Some(player) =>
              player.copy(
                player_id = Some(k),
                GDPR = login.GDPR.orElse(player.GDPR),
                GlobalProductClassification = login.GlobalProductClassification,
                ProductClassification = login.ProductClassification,
                FavoriteGameIds = login.FavoriteGameIds,
                FavoriteGameNames = login.FavoriteGameNames,
                IsVerified = login.IsVerified.orElse(player.IsVerified),
                block_reason = login.BlockReason.orElse(player.block_reason),
                VIPLevel = login.VIPLevel.orElse(player.VIPLevel),
                VIPMaxLevel = login.VIPMaxLevel.orElse(player.VIPMaxLevel),
                BlockedCasino =
                  login.BlockedCasino.orElse(player.BlockedCasino),
                BlockedSport = login.BlockedSport.orElse(player.BlockedSport),
                CanBeContactedBySMS = login.CanBeContactedBySMS.orElse(
                  player.CanBeContactedBySMS
                ),
                CanBeContactedByEmail = login.CanBeContactedByEmail
                  .orElse(player.CanBeContactedByEmail),
                CanBeContactedByPhoneOrDynamicMessage =
                  login.CanBeContactedByPhoneOrDynamicMessage.orElse(
                    player.CanBeContactedByPhoneOrDynamicMessage
                  ),
                CanReceivePromotions = login.CanReceivePromotions.orElse(
                  player.CanReceivePromotions
                ),
                BonusProgram = login.BonusProgram.orElse(
                  player.BonusProgram
                ),
                IsOptInCashback =
                  login.IsOptInCashback.orElse(player.IsOptInCashback),
                IsOptInWeeklyReload = login.IsOptInWeeklyReload.orElse(
                  player.IsOptInWeeklyReload
                ),
                IsOptInWeekendReload = login.IsOptInWeekendReload
                  .orElse(player.IsOptInWeekendReload),
                IsOptInForLastFourPeriodsCashback =
                  login.IsOptInForLastFourPeriodsCashback.orElse(
                    player.IsOptInForLastFourPeriodsCashback
                  ),
                IsOptInForLastFourPeriodsWeeklyReload =
                  login.IsOptInForLastFourPeriodsWeeklyReload.orElse(
                    player.IsOptInForLastFourPeriodsWeeklyReload
                  ),
                IsOptInForLastFourPeriodsWeekendReload =
                  login.IsOptInForLastFourPeriodsWeekendReload.orElse(
                    player.IsOptInForLastFourPeriodsWeekendReload
                  )
              )
            case _ =>
              PlayerStore(
                player_id = Some(k),
                GDPR = login.GDPR,
                GlobalProductClassification = login.GlobalProductClassification,
                ProductClassification = login.ProductClassification,
                FavoriteGameIds = login.FavoriteGameIds,
                FavoriteGameNames = login.FavoriteGameNames,
                IsVerified = login.IsVerified,
                block_reason = login.BlockReason,
                VIPLevel = login.VIPLevel,
                VIPMaxLevel = login.VIPMaxLevel,
                BlockedCasino = login.BlockedCasino,
                BlockedSport = login.BlockedSport,
                CanBeContactedBySMS = login.CanBeContactedBySMS,
                CanBeContactedByEmail = login.CanBeContactedByEmail,
                CanBeContactedByPhoneOrDynamicMessage =
                  login.CanBeContactedByPhoneOrDynamicMessage,
                CanReceivePromotions = login.CanReceivePromotions,
                BonusProgram = login.BonusProgram,
                IsOptInCashback = login.IsOptInCashback,
                IsOptInWeeklyReload = login.IsOptInWeeklyReload,
                IsOptInWeekendReload = login.IsOptInWeekendReload,
                IsOptInForLastFourPeriodsCashback =
                  login.IsOptInForLastFourPeriodsCashback,
                IsOptInForLastFourPeriodsWeeklyReload =
                  login.IsOptInForLastFourPeriodsWeeklyReload,
                IsOptInForLastFourPeriodsWeekendReload =
                  login.IsOptInForLastFourPeriodsWeekendReload
              )
          }

          playerKVStore.put(k, p)

        }
        null
      case _ =>
        logger.debug(s"${processorContext.topic()} not handled for now")
        null
    }

    if (playerToSaveInStore != null) {

      val firstModification =
        sendEmailConfirmed(playerFromStore, playerToSaveInStore)
      val toUsed = sendPhoneConfirmed(playerFromStore, firstModification)

      playerKVStore.put(k, toUsed)

      if (toUsed != playerFromStore) {

        sender.sendPlayer(
          Some(processorContext.topic()),
          Option(playerFromStore),
          toUsed
        )
        if (
          toUsed.is_self_excluded.isDefined && toUsed.is_self_excluded.get == "true" && toUsed.brand_id.isDefined
        ) {
          val userBlockedMessage =
            s"""
                 |{
                 |"type": "USER_BLOCKED",
                 |"mappingSelector": "self_exclusion",
                 |"contactId": "$k"
                 |}
                 |""".stripMargin
          // send blocked message
          sender.sendToSQS(
            userBlockedMessage,
            k,
            toUsed.brand_id.get
          )
        }

        if (
          toUsed.failed_deposit_date.isDefined && (playerFromStore == null || toUsed.failed_deposit_date != playerFromStore.failed_deposit_date) && toUsed.brand_id.isDefined
        ) {
          val failedDepositMessage =
            s"""
                 |{"type": "GENERIC_USER",
                 |"mappingSelector": "failed_deposit",
                 |"contactId": "$k",
                 |"properties": {
                 | "failed_deposit_date": "${toUsed.failed_deposit_date
                .map(keepYYYYMMDD(_))
                .get}"
                 |}
                 |}
                 |""".stripMargin
          // send blocked message
          sender.sendToSQS(
            failedDepositMessage,
            k,
            toUsed.brand_id.get
          )
        }

        if (
          toUsed.reg_datetime.isDefined && (playerFromStore == null || toUsed.reg_datetime
            .getOrElse("") != playerFromStore.reg_datetime.getOrElse(
            ""
          )) && toUsed.brand_id.isDefined
        ) {
          val registrationMessage =
            s"""
                 |{"type": "GENERIC_USER",
                 |"mappingSelector": "player_registered",
                 |"contactId": "$k",
                 |"properties": {
                 | "reg_datetime": "${toUsed.reg_datetime
                .map(keepYYYYMMDD(_))
                .get}"
                 |}
                 |}
                 |""".stripMargin
          // send blocked message
          sender.sendToSQS(
            registrationMessage,
            k,
            toUsed.brand_id.get
          )
        }
      } else {
        logger.debugv("same", Array(kv("key", k)): _*)
      }
    }
    new KeyValue(k, playerToSaveInStore)

  }

  def sendPhoneConfirmed(
      previous: PlayerStore,
      ps: PlayerStore
  ): PlayerStore = {
    if (ps.IsPhoneConfirmedSent.isEmpty) {
      val isTrue = ps.IsPhoneConfirmed
        .map(_.toLowerCase())
        .contains("true")

      val isAfter = ps.reg_datetime
        .map(keepYYYYMMDD(_))
        .filter(v => v >= "2025-05-05")
        .isDefined

      if (isTrue && isAfter) {
        val message =
          s"""
                 |{"type": "GENERIC_USER",
                 |"mappingSelector": "player_PhoneConfirmed",
                 |"contactId": "${ps.player_id.get}",
                 |"properties": {"IsPhoneConfirmed": "true", "player_id":"${ps.player_id.get}", "brand_id":"${ps.brand_id.get}", "registrationSource":"${ps.PhoneConfirmedRegistrationSource
              .getOrElse("")}"}
                 |}
                 |""".stripMargin
        sender.sendToSQS(
          message,
          ps.player_id.get,
          ps.brand_id.get
        )
        ps.copy(IsPhoneConfirmedSent = Some(true))
      } else {
        ps
      }
    } else {
      ps
    }

  }

  def sendEmailConfirmed(
      previous: PlayerStore,
      ps: PlayerStore
  ): PlayerStore = {
    if (ps.IsEmailConfirmedSent.isEmpty) {
      val isTrue = ps.IsEmailConfirmed
        .map(_.toLowerCase())
        .contains("true")

      val isAfter = ps.reg_datetime
        .map(keepYYYYMMDD(_))
        .filter(v => v >= "2025-05-05")
        .isDefined

      if (isTrue && isAfter) {
        val message =
          s"""
                 |{"type": "GENERIC_USER",
                 |"mappingSelector": "player_EmailConfirmed",
                 |"contactId": "${ps.player_id.get}",
                 |"properties": {"IsEmailConfirmed": "true", "player_id":"${ps.player_id.get}", "brand_id":"${ps.brand_id.get}", "registrationSource":"${ps.EmailConfirmedRegistrationSource
              .getOrElse("")}"}
                 |}
                 |""".stripMargin
        sender.sendToSQS(
          message,
          ps.player_id.get,
          ps.brand_id.get
        )
        ps.copy(IsEmailConfirmedSent = Some(true))
      } else {
        ps
      }
    } else {
      ps
    }

  }

  def sendIsVerified(
      playerFromStore: PlayerStore,
      update: PlayerStore,
      verification: Verification
  ) = {
    val isFirst =
      playerFromStore == null || playerFromStore.IsVerifiedFromVerification.isEmpty || playerFromStore.IsVerifiedFromVerification
        .map(_.toLowerCase())
        .contains("false") || playerFromStore.IsVerifiedFromVerification
        .map(_.trim())
        .contains("")
    val isTrue = update.IsVerifiedFromVerification
      .map(_.toLowerCase())
      .contains("true")

    if (isFirst && isTrue) {
      import VerificationSQS._
      val body = Body[VerificationSQS](
        "GENERIC_USER",
        verification.player_id.get,
        "player_isverified",
        VerificationSQS(verification)
      )
      sender.sendToSQS(
        printer.print(Body.bodyEncoder[VerificationSQS].apply(body)),
        verification.player_id.get,
        verification.brand_id.get
      )
    }

  }
  def getKVStore[K, V](storeName: String): KeyValueStore[K, V] = {
    Try {
      processorContext
        .getStateStore(storeName)
        .asInstanceOf[KeyValueStore[K, V]]
    } match {
      case Success(kvStore) => kvStore
      case Failure(e: ClassCastException) =>
        throw new IllegalArgumentException(
          s"Please provide a KeyValueStore for $storeName, reason: $e"
        )
      case Failure(e) => throw e
    }
  }

  override def close(): Unit = {}

}
