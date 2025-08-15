package com.soft2bet.model

case class Verification(
    player_id: Option[String],
    brand_id: Option[String],
    IsVerified: Option[String],
    VerificationStatus: Option[String],
    IsEmailConfirmed: Option[String],
    IsPhoneConfirmed: Option[String],
    registrationSource: Option[String],
)

object Verification {
  import io.circe.Encoder
  import io.circe.generic.semiauto._

  implicit val verificationEncoder: Encoder[Verification] =
    deriveEncoder
}

case class VerificationSQS(
    player_id: Option[String],
    brand_id: Option[String],
    IsVerified: Option[String],
    VerificationStatus: Option[String],
    RegistrationSource: Option[String]
)

object VerificationSQS {
  import io.circe.Encoder
  import io.circe.generic.semiauto._

  implicit val encoder: Encoder[VerificationSQS] =
    deriveEncoder

  def apply(v: Verification): VerificationSQS = {
    VerificationSQS(
      player_id = v.player_id,
      brand_id = v.brand_id,
      IsVerified = v.IsVerified,
      VerificationStatus = v.VerificationStatus,
      RegistrationSource = v.registrationSource,
    )
  }

}
