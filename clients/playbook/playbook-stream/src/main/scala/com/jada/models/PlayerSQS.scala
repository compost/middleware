package com.jada.models

import com.jada.processor.MappingTransformer.toBoolean
import org.apache.kafka.streams.state.KeyValueStore

import java.util.Locale

case class PlayerSQS(
    player_id: Option[String],
    brand_id: Option[String],
    reg_datetime: Option[String],
    first_name: Option[String],
    last_name: Option[String],
    email: Option[String],
    phone_number: Option[String],
    language: Option[String],
    affiliate_id: Option[String],
    is_self_excluded: Option[Boolean],
    first_dep_datetime: Option[String],
    dob: Option[String],
    vip: Option[String],
    test_user: Option[String],
    currency: Option[String],
    country: Option[String],
    marketing_phone: Option[String],
    is_playbreak: Option[Boolean],
    playbreak_expiry: Option[String],
    postcode: Option[String],
    promo_id: Option[String],
    promo_banned: Option[String],
    city: Option[String],
    is_deposited_restricted: Option[String]
)

object PlayerSQS {

  def deserialize(data: Array[Byte]): PlayerSQS = {
    import io.circe._
    import io.circe.generic.auto._
    parser.decode[PlayerSQS](new String(data)).right.get
  }

  def apply(
      player: PlayerStore,
      currencies: KeyValueStore[String, Currency],
      countries: KeyValueStore[String, Country]
  ): PlayerSQS = {

    val is_deposited_restricted = player.brand_id match {
      case Some("238" | "239") => player.is_deposited_restricted
      case _                   => None
    }
    new PlayerSQS(
      player_id = player.player_id,
      brand_id = player.brand_id,
      reg_datetime = player.reg_datetime.map(_.substring(0, 10)),
      first_name = player.first_name,
      last_name = player.last_name,
      email = player.email,
      phone_number = player.phone_number,
      language = player.language,
      affiliate_id = player.affiliate_id,
      is_self_excluded = player.is_self_excluded.map(toBoolean),
      first_dep_datetime = player.first_dep_datetime,
      dob = player.dob,
      vip = player.VIP,
      test_user = player.test_user,
      currency = player.currency_description,
      country = player.country_description,
      marketing_phone = player.phone_consented,
      is_playbreak = player.is_playbreak.map(toBoolean),
      playbreak_expiry = player.playbreak_expiry.map(_.substring(0, 10)),
      postcode = player.postcode,
      promo_id = player.promo_id,
      promo_banned = player.promo_banned,
      city = player.city,
      is_deposited_restricted = is_deposited_restricted
    )
  }
}
