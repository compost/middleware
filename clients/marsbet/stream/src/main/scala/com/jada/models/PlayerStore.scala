package com.jada.models

import java.time.Instant
import java.text.SimpleDateFormat

case class PlayerStore(
    player_id: Option[String] = None,
    reg_datetime: Option[String] = None,
    first_name: Option[String] = None,
    last_name: Option[String] = None,
    email: Option[String] = None,
    phone_number: Option[String] = None,
    language: Option[String] = None,
    affiliate_id: Option[String] = None,
    first_dep_datetime: Option[String] = None,
    dob: Option[String] = None,
    country_id: Option[String] = None,
    country_description: Option[String] = None,
    VIP: Option[String] = None,
    test_user: Option[String] = None,
    currency_id: Option[String] = None,
    currency_description: Option[String] = None,
    brand_id: Option[String] = None
)
object PlayerStore {

  import io.circe.Encoder
  import io.circe.generic.semiauto._

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
          first_dep_datetime =
            player.first_dep_datetime.orElse(stored.first_dep_datetime),
          dob = player.dob.orElse(stored.dob),
          VIP = player.VIP.orElse(stored.VIP),
          test_user = player.test_user.orElse(stored.test_user),
          currency_id = player.currency_id.orElse(stored.currency_id),
          country_id = player.country_id.orElse(stored.country_id),
          currency_description =
            player.currency_description.orElse(stored.currency_description),
          country_description =
            player.country_description.orElse(stored.country_description)
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
          first_dep_datetime = player.first_dep_datetime,
          dob = player.dob,
          VIP = player.VIP,
          test_user = player.test_user,
          currency_id = player.currency_id,
          country_id = player.country_id,
          currency_description = player.currency_description,
          country_description = player.country_description
        )
    }
  }

  import java.time.LocalDate
  import java.time.format.DateTimeFormatter
  def convertDateFormat(
      dateStr: String
  ): String = {
    val inputFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy")
    val outputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

    val date = LocalDate.parse(dateStr, inputFormatter)
    date.format(outputFormatter)
  }
  def apply(ps: Option[PlayerStore], player: PlayerDatabase): PlayerStore = {
    ps match {
      case Some(stored) =>
        stored.copy(
          player_id = player.PLAYER_ID.orElse(stored.player_id),
          brand_id = player.BRAND_ID.orElse(stored.brand_id),
          reg_datetime = player.REG_DATETIME.orElse(stored.reg_datetime),
          first_name = player.FIRST_NAME.orElse(stored.first_name),
          last_name = player.LAST_NAME.orElse(stored.last_name),
          email = player.EMAIL.orElse(stored.email),
          phone_number = player.PHONE_NUMBER.orElse(stored.phone_number),
          language = player.LANGUAGE.orElse(stored.language),
          affiliate_id = player.AFFILIATE_ID.orElse(stored.affiliate_id),
          first_dep_datetime =
            player.FIRST_DEP_DATETIME.orElse(stored.first_dep_datetime),
          dob = player.DOB.map(convertDateFormat(_)).orElse(stored.dob),
          VIP = player.VIP.orElse(stored.VIP),
          test_user = player.TEST_USER.orElse(stored.test_user),
          currency_description =
            player.CURRENCY.orElse(stored.currency_description),
          country_description =
            player.COUNTRY.orElse(stored.country_description)
        )
      case None =>
        PlayerStore(
          player_id = player.PLAYER_ID,
          brand_id = player.BRAND_ID,
          reg_datetime = player.REG_DATETIME,
          first_name = player.FIRST_NAME,
          last_name = player.LAST_NAME,
          email = player.EMAIL,
          phone_number = player.PHONE_NUMBER,
          language = player.LANGUAGE,
          affiliate_id = player.AFFILIATE_ID,
          first_dep_datetime = player.FIRST_DEP_DATETIME,
          dob = player.DOB.map(convertDateFormat(_)),
          VIP = player.VIP,
          test_user = player.TEST_USER,
          currency_description = player.CURRENCY,
          country_description = player.COUNTRY
        )
    }
  }

}
