package com.soft2bet.model

import com.soft2bet.Common

case class SportPush(
    player_id: Option[String],
    brand_id: Option[String],
    `type`: Option[String],
    message: Option[String]
)

case class SportPushSQS(
    player_id: Option[String],
    brand_id: Option[String],
    sportPushType: Option[String],
    sportPushMessage: Option[String]
)

object SportPushSQS {
  import io.circe.Encoder
  import io.circe.generic.semiauto._
  implicit val sportPushSQSEncoder: Encoder[SportPushSQS] =
    deriveEncoder

  def apply(s: SportPush): SportPushSQS = {
    SportPushSQS(
      player_id = s.player_id,
      brand_id = s.brand_id,
      sportPushType = s.`type`,
      sportPushMessage = s.message
    )
  }
}
object SportPush {
  import io.circe.Encoder
  import io.circe.generic.semiauto._
  implicit val sportPushEncoder: Encoder[SportPush] =
    deriveEncoder
}
