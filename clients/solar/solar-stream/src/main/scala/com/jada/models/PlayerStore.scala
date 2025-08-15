package com.jada.models

import java.time.Instant

case class PlayerStore(
    player_id: Option[String] = None,
    reg_datetime: Option[String] = None,
    brand_id: Option[String] = None,
    brand_name: Option[String] = None,
    first_name: Option[String] = None,
    last_name: Option[String] = None,
    email: Option[String] = None,
    phone_number: Option[String] = None,
    language: Option[String] = None,
    affiliate_id: Option[String] = None,
    is_self_excluded: Option[String] = None,
    first_dep_datetime: Option[String] = None,
    dob: Option[String] = None,
    country_id: Option[String] = None,
    vip: Option[String] = None,
    vip_level: Option[String] = None,
    currency_id: Option[String] = None,
    test_user: Option[String] = None,
    isInit: Boolean = true,
    actionTriggers: List[ActionTrigger] = List.empty,
    consent_email: Option[String] = None,
    consent_sms: Option[String] = None
)
object PlayerStore {

  import io.circe.Encoder
  import io.circe.generic.semiauto._

  implicit val actionTriggerEncoder: Encoder[ActionTrigger] = deriveEncoder
  implicit val playerStoreEncoder: Encoder[PlayerStore] = deriveEncoder

}
