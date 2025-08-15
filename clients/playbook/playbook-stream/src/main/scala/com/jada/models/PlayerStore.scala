package com.jada.models

import java.time.Instant
import java.text.SimpleDateFormat

case class PlayerStore(
    player_id: Option[String] = None,
    brand_id: Option[String] = None,
    reg_datetime: Option[String] = None,
    first_name: Option[String] = None,
    last_name: Option[String] = None,
    email: Option[String] = None,
    phone_number: Option[String] = None,
    language: Option[String] = None,
    affiliate_id: Option[String] = None,
    is_self_excluded: Option[String] = None,
    first_dep_datetime: Option[String] = None,
    dob: Option[String] = None,
    VIP: Option[String] = None,
    test_user: Option[String] = None,
    currency_description: Option[String] = None,
    country_description: Option[String] = None,
    postcode: Option[String] = None,
    promo_id: Option[String] = None,
    promo_banned: Option[String] = None,
    city: Option[String] = None,
    is_deposited_restricted: Option[String] = None,
    // user consent update
    email_consented: Option[String] = None,
    sms_consented: Option[String] = None,
    phone_consented: Option[String] = None,
    // status of churn
    sent_customer_churn: Boolean = false,
    last_activity_date: Option[Instant] = None,
    wallet_transaction_type_id: Option[String] = None,
    wallet_transaction_status_id: Option[String] = None,
    wagering_transaction_type_id: Option[String] = None,
    wagering_ggr_amount: Option[String] = None,
    wagering_has_resulted: Option[String] = None,
    // login
    last_login_datetime: Option[String] = None,
    last_login_success: Option[String] = None,
    status: Option[String] = None,
    is_playbreak: Option[String] = None,
    playbreak_expiry: Option[String] = None,
    // to know whether to send player_update
    players_data_received: Option[String] = None,
    sf_inplay: Option[String] = None,
    sf_inplay_casino: Option[String] = None,
    sf_inplay_livecasino: Option[String] = None,
    sf_inplay_lottery: Option[String] = None,
    sf_inplay_virtualsports: Option[String] = None,
    sf_prematch: Option[String] = None,
    sf_prematch_casino: Option[String] = None,
    sf_prematch_livecasino: Option[String] = None,
    sf_prematch_lottery: Option[String] = None,
    sf_prematch_virtualsports: Option[String] = None,
    player_consent_multi: Option[PlayerConsentMulti] = None
)
object PlayerStore {

  import io.circe.Encoder
  import io.circe.generic.semiauto._

  implicit val playerConsentMultiEncoder: Encoder[PlayerConsentMulti] =
    deriveEncoder
  implicit val playerStoreEncoder: Encoder[PlayerStore] = deriveEncoder

  def apply(ps: Option[PlayerStore], player: Player): PlayerStore = {
    ps match {
      case Some(stored) =>
        stored.copy(
          player_id = player.player_id.orElse(stored.player_id),
          brand_id = player.brand_id.orElse(stored.brand_id),
          reg_datetime = player.reg_datetime.orElse(stored.reg_datetime),
          first_name = player.first_name.orElse(stored.first_name),
          last_name = player.last_name.orElse(stored.last_name),
          email = player.email.orElse(stored.email),
          phone_number = player.phone_number.orElse(stored.phone_number),
          language = player.language.orElse(stored.language),
          affiliate_id = player.affiliate_id.orElse(stored.affiliate_id),
          is_self_excluded =
            player.is_self_excluded.orElse(stored.is_self_excluded),
          first_dep_datetime =
            player.first_dep_datetime.orElse(stored.first_dep_datetime),
          dob = player.dob.orElse(stored.dob),
          VIP = player.VIP.orElse(stored.VIP),
          test_user = player.test_user.orElse(stored.test_user),
          currency_description =
            player.currency_description.orElse(stored.currency_description),
          country_description =
            player.country_description.orElse(stored.country_description),
          is_playbreak = player.is_playbreak.orElse(stored.is_playbreak),
          playbreak_expiry =
            player.playbreak_expiry.orElse(stored.playbreak_expiry),
          players_data_received = Some("true"),
          postcode = player.postcode.orElse(stored.postcode),
          promo_id = player.promo_id.orElse(stored.promo_id),
          promo_banned = player.promo_banned.orElse(stored.promo_banned),
          city = player.city.orElse(stored.city),
          is_deposited_restricted = player.is_deposited_restricted.orElse(
            stored.is_deposited_restricted
          )
        )
      case None =>
        PlayerStore(
          player_id = player.player_id,
          brand_id = player.brand_id,
          reg_datetime = player.reg_datetime,
          first_name = player.first_name,
          last_name = player.last_name,
          email = player.email,
          phone_number = player.phone_number,
          language = player.language,
          affiliate_id = player.affiliate_id,
          is_self_excluded = player.is_self_excluded,
          first_dep_datetime = player.first_dep_datetime,
          dob = player.dob,
          VIP = player.VIP,
          test_user = player.test_user,
          currency_description = player.currency_description,
          country_description = player.country_description,
          is_playbreak = player.is_playbreak,
          playbreak_expiry = player.playbreak_expiry,
          players_data_received = Some("true"),
          postcode = player.postcode,
          promo_id = player.promo_id,
          promo_banned = player.promo_banned,
          city = player.city,
          is_deposited_restricted = player.is_deposited_restricted
        )
    }
  }

  def apply(ps: Option[PlayerStore], player: PlayerDB): PlayerStore = {
    ps match {
      case Some(stored) =>
        stored.copy(
          postcode = player.POSTCODE,
          promo_id = player.PROMO_ID,
          promo_banned = player.PROMO_BANNED,
          city = player.CITY
        )
      case None =>
        PlayerStore(
          player_id = player.PLAYER_ID,
          brand_id = player.BRAND_ID,
          postcode = player.POSTCODE,
          promo_id = player.PROMO_ID,
          promo_banned = player.PROMO_BANNED,
          city = player.CITY
        )
    }

  }

  def apply(
      ps: Option[PlayerStore],
      userConsentUpdate: UserConsentUpdate
  ): PlayerStore = {
    ps match {
      case Some(stored) =>
        stored.copy(
          email_consented = userConsented(userConsentUpdate, "email").orElse(
            stored.email_consented
          ),
          sms_consented = userConsented(userConsentUpdate, "sms").orElse(
            stored.sms_consented
          ),
          phone_consented = userConsented(userConsentUpdate, "phone").orElse(
            stored.phone_consented
          )
        )
      case None =>
        PlayerStore(
          player_id = userConsentUpdate.player_id,
          brand_id = userConsentUpdate.brand_id,
          email_consented = userConsented(userConsentUpdate, "email"),
          sms_consented = userConsented(userConsentUpdate, "sms"),
          phone_consented = userConsented(userConsentUpdate, "phone")
        )
    }

  }

  def apply(
      ps: Option[PlayerStore],
      login: Login
  ): PlayerStore = {
    ps match {
      case Some(stored) =>
        stored.copy(
          player_id = login.player_id.orElse(stored.player_id),
          brand_id = login.brand_id.orElse(stored.brand_id),
          last_login_datetime =
            login.login_datetime.orElse(stored.last_login_datetime),
          last_login_success =
            login.login_success.orElse(stored.last_login_success)
        )
      case None =>
        PlayerStore(
          player_id = login.player_id,
          brand_id = login.brand_id,
          last_login_datetime = login.login_datetime,
          last_login_success = login.login_success
        )
    }

  }

  def apply(
      ps: Option[PlayerStore],
      status: PlayerStatusDB
  ): PlayerStore = {
    ps match {
      case Some(stored) =>
        stored.copy(
          player_id = status.PLAYER_ID.orElse(stored.player_id),
          brand_id = status.BRAND_ID.orElse(stored.brand_id),
          is_playbreak = status.IS_PLAYBREAK.orElse(stored.is_playbreak),
          playbreak_expiry =
            status.PLAYBREAK_EXPIRY.orElse(stored.playbreak_expiry),
          is_self_excluded =
            status.IS_SELF_EXCLUDED.orElse(stored.is_self_excluded),
          status = parseStatus(status).orElse(stored.status),
          players_data_received = Some("true")
        )
      case None =>
        PlayerStore(
          player_id = status.PLAYER_ID,
          brand_id = status.BRAND_ID,
          playbreak_expiry = status.PLAYBREAK_EXPIRY,
          is_playbreak = status.IS_PLAYBREAK,
          is_self_excluded = status.IS_SELF_EXCLUDED,
          status = parseStatus(status),
          players_data_received = Some("true")
        )
    }

  }

  // 2-self-exclusion
  // 2-playbreak
  // 2 3 4 5 => blocked
  // 1
  def parseStatus(statusDB: PlayerStatusDB): Option[String] = {
    statusDB match {
      case PlayerStatusDB(_, _, Some("1"), _, _, _, _) => Some("1")
      case PlayerStatusDB(_, _, _, _, _, _, Some(v)) if toBoolean(v) =>
        Some("self-exclusion")
      case PlayerStatusDB(_, _, Some("2"), _, Some(v), _, _) if toBoolean(v) =>
        Some("2-playbreak")
      case PlayerStatusDB(_, _, Some("2" | "3" | "4" | "5"), _, _, _, _) =>
        Some("blocked")
      case _ => None
    }
  }

  def toBoolean(v: String): Boolean = {
    v match {
      case "true"  => true
      case "false" => false
      case "0"     => false
      case "1"     => true
      case _       => false
    }
  }

  def userConsented(
      userConsentUpdate: UserConsentUpdate,
      kind: String
  ): Option[String] = {
    userConsentUpdate.channel
      .filter(_.toLowerCase().equals(kind))
      .flatMap(_ => userConsentUpdate.consented)
  }
}
