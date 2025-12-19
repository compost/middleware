package com.soft2bet.model

case class OnePassVerification(
    player_id: Option[String],
    brand_id: Option[String],
    IsVerified: Option[String],
    VerificationStatus: Option[String]
)

case class OnePassVerificationSQS(
    playerid: Option[String],
    brandId: Option[String],
    isverified: Option[String],
    verificationstatus: Option[String]
)

object OnePassVerificationSQS {
  def apply(
      wallet: OnePassVerification
  ): OnePassVerificationSQS = {
    OnePassVerificationSQS(
      playerid = wallet.player_id,
      brandId = wallet.brand_id,
      isverified = wallet.IsVerified,
      verificationstatus = wallet.VerificationStatus
    )
  }
}
