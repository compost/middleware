package com.soft2bet.model

case class VipLevel(
    player_id: Option[String],
    brand_id: Option[String],
    VIPLevel25: Option[String],
    VIPLevelIncrease: Option[String],
    CurrentVIPLevelStartDate: Option[String],
    VIP25: Option[String],
    PointsToNextVIPLevel: Option[String],
    PointsRequiredToMaintainVIPLevel: Option[String],
    SafeVIPLevel: Option[String],
    VIPMaxLevel25: Option[String],
    LoyaltyProgramType: Option[String]
)

case class VipLevelSQS(
    originalId: Option[String],
    brandId: Option[String],
    VIPLevel25: Option[String],
    VIPLevelIncrease: Option[String],
    CurrentVIPLevelStartDate: Option[String],
    VIP25: Option[String],
    PointsToNextVIPLevel: Option[String],
    PointsRequiredToMaintainVIPLevel: Option[String],
    SafeVIPLevel: Option[String],
    VIPMaxLevel25: Option[String],
    LoyaltyProgramType: Option[String]
)
case class VipLevel25ChangedSQS(
    playerid: Option[String],
    brandId: Option[String]
)

object VipLevelSQS {
  def apply(v: VipLevel): VipLevelSQS = new VipLevelSQS(
    originalId = v.player_id,
    brandId = v.brand_id,
    VIPLevel25 = v.VIPLevel25,
    VIPLevelIncrease = v.VIPLevelIncrease,
    CurrentVIPLevelStartDate = v.CurrentVIPLevelStartDate,
    VIP25 = v.VIP25,
    PointsToNextVIPLevel = v.PointsToNextVIPLevel,
    PointsRequiredToMaintainVIPLevel = v.PointsRequiredToMaintainVIPLevel,
    SafeVIPLevel = v.SafeVIPLevel,
    VIPMaxLevel25 = v.VIPMaxLevel25,
    LoyaltyProgramType = v.LoyaltyProgramType
  )
}

object VipLevel25ChangedSQS {
  def apply(v: VipLevel): VipLevel25ChangedSQS = new VipLevel25ChangedSQS(
    playerid = v.player_id,
    brandId = v.brand_id
  )
}
