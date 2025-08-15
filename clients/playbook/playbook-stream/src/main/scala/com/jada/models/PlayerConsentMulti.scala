package com.jada.models

case class PlayerConsentMulti(
    player_id: Option[String],
    brand_id: Option[String],
    email_casino: Option[String],
    email_betting: Option[String],
    email_bingo: Option[String],
    sms_casino: Option[String],
    sms_betting: Option[String],
    sms_bingo: Option[String]
)

case class PlayerConsentMultiSQS(
    `type`: String,
    contactId: String,
    mappingSelector: String,
    consented: Boolean,
    channel: String,
    properties: PlayerConsentMulti
)
object PlayerConsentMulti {
  import io.circe.Encoder
  import io.circe.generic.semiauto._
  implicit val encoder: Encoder[PlayerConsentMulti] = deriveEncoder
}
object PlayerConsentMultiSQS {
  import io.circe.Encoder
  import io.circe.generic.semiauto._
  import PlayerConsentMulti._
  implicit val encoder: Encoder[PlayerConsentMultiSQS] = deriveEncoder
  val Type = "USER_CONSENT_UPDATE"
  def apply(
      p: PlayerConsentMulti,
      mp: String
  ): List[PlayerConsentMultiSQS] = {

    val consentedSms =
      p.sms_betting.map(_.toBoolean).getOrElse(false) || p.sms_bingo
        .map(_.toBoolean)
        .getOrElse(false) || p.sms_casino.map(_.toBoolean).getOrElse(false)
    val consentedEmail =
      p.email_betting.map(_.toBoolean).getOrElse(false) || p.email_bingo
        .map(_.toBoolean)
        .getOrElse(false) || p.email_casino.map(_.toBoolean).getOrElse(false)

    List(
      p.sms_betting.map(_ => {
        PlayerConsentMultiSQS(
          `type` = Type,
          contactId = p.player_id.get,
          mappingSelector = mp,
          consented = consentedSms,
          channel = "SMS",
          properties = p
        )
      }),
      p.email_betting.map(_ => {
        PlayerConsentMultiSQS(
          `type` = Type,
          contactId = p.player_id.get,
          mappingSelector = mp,
          consented = consentedEmail,
          channel = "EMAIL",
          properties = p
        )
      })
    ).flatten
  }
}
