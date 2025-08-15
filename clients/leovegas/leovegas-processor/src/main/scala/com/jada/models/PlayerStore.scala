package com.jada.models

import java.time.Instant

case class PlayerStore(
    player_id: Option[String] = None,
    // players
    brand_id: Option[String] = None,
    reg_datetime: Option[Instant] = None,
    first_name: Option[String] = None,
    last_name: Option[String] = None,
    email: Option[String] = None,
    phone_number: Option[String] = None,
    is_self_excluded: Option[String] = None,
    country_id: Option[String] = None,
    vip: Option[String] = None,
    currency_id: Option[String] = None,
    test_user: Option[String] = None,
    // wagering
    betsAndResults: List[BetAndResult] = List.empty,
    // player extended
    licenseuid: Option[String] = None,
    operator: Option[String] = None,
    country: Option[String] = None,
    wagering: Option[Wagering] = None,
    // internal
    processed: Boolean = false,
    cleaned: Boolean = false
)

case class BetAndResult(
    betId: Option[String],
    betDatetime: Option[Instant],
    resultDatetime: Option[Instant],
    hasResulted: Boolean,
    resultId: Option[String],
    vertical_id: Option[String],
    ggr_amount: Option[String]
)

object PlayerStore {

  import io.circe.Encoder
  import io.circe.generic.semiauto._
  implicit val wageringEncoder: Encoder[Wagering] = deriveEncoder
  implicit val betAndResultEncoder: Encoder[BetAndResult] = deriveEncoder
  implicit val playerStoreEncoder: Encoder[PlayerStore] = deriveEncoder

}
