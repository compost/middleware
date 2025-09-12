package com.soft2bet.model

case class Login(
    player_id: Option[String],
    login_datetime: Option[String],
    brand_id: Option[String],
    brand_name: Option[String],
    login_success: Option[String],
    GlobalProductClassification: Option[String],
    ProductClassification: Option[String],
    FavoriteGameIds: Option[List[Option[String]]],
    FavoriteGameNames: Option[List[Option[String]]],
    BlockReason: Option[String],
    IsBlocked: Option[String],
    IsVerified: Option[String],
    VIPLevel: Option[String],
    VIPMaxLevel: Option[String],
    BlockedCasino: Option[String],
    BlockedSport: Option[String],
    CanBeContactedBySMS: Option[String],
    CanBeContactedByEmail: Option[String],
    CanBeContactedByPhoneOrDynamicMessage: Option[String],
    CanReceivePromotions: Option[String],
    BonusProgram: Option[String],
    GDPR: Option[String],
    IsOptInCashback: Option[String],
    IsOptInWeeklyReload: Option[String],
    IsOptInWeekendReload: Option[String],
    IsOptInForLastFourPeriodsCashback: Option[String],
    IsOptInForLastFourPeriodsWeeklyReload: Option[String],
    IsOptInForLastFourPeriodsWeekendReload: Option[String]
)

case class LoginJSON(
    lastLoginDate: Option[String] = None
)


object LoginJSON {

  import io.circe.Encoder
  import io.circe.generic.semiauto._

  def apply(w: Login): LoginJSON = {
    LoginJSON(
      lastLoginDate = w.login_datetime.map(keepYYYYMMDD(_))
    )
  }
  implicit val loginSQSEncoder: Encoder[LoginJSON] = deriveEncoder
}
