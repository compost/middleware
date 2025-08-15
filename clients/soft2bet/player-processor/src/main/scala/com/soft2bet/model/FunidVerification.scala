package com.soft2bet.model

case class FunidVerification(
    player_id: Option[String],
    brand_id: Option[String],
    IsVerified: Option[String],
    VerificationStatus: Option[String]
)

case class FunidVerificationSQS(
    playerid: Option[String],
    brandId: Option[String],
    isverified: Option[String],
    verificationstatus: Option[String]
)

object FunidVerificationSQS {
  def apply(
      wallet: FunidVerification
  ): FunidVerificationSQS = {
    FunidVerificationSQS(
      playerid = wallet.player_id,
      brandId = wallet.brand_id,
      isverified = wallet.IsVerified,
      verificationstatus = wallet.VerificationStatus
    )
  }
}
