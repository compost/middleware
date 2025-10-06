package com.soft2bet.model

case class PlayerStore(
    // players
    player_id: Option[String] = None,
    first_name: Option[String] = None,
    last_name: Option[String] = None,
    email: Option[String] = None,
    phone_number: Option[String] = None,
    is_self_excluded: Option[String] = None,
    vip: Option[String] = None,
    reg_datetime: Option[String] = None,
    language: Option[String] = None,
    affiliate_id: Option[String] = None,
    first_dep_datetime: Option[String] = None,
    dob: Option[String] = None,
    country_id: Option[String] = None,
    is_suppressed: Option[String] = None,
    brand_id: Option[String] = None,
    brand_name: Option[String] = None,
    Postcode: Option[String] = None,
    Country: Option[String] = None,
    IsSelfExcludedOtherBrand: Option[String] = None,
    AccountType: Option[String] = None,
    BonusProgram: Option[String] = None,
    Currency: Option[String] = None,
    Duplicate: Option[String] = None,
    GDPR: Option[String] = None,
    Gender: Option[String] = None,
    ReferralType: Option[String] = None,
    RegistrationPlatform: Option[String] = None,
    registrationSource: Option[String] = None,
    Username: Option[String] = None,
    IsEmailValid: Option[String] = None,
    IsVerified: Option[String] = None,
    IsVerifiedFromVerification: Option[String] = None,
    IsEmailConfirmed: Option[String] = None,
    EmailConfirmedRegistrationSource: Option[String] = None,
    IsEmailConfirmedSent: Option[Boolean] = None,
    IsPhoneConfirmed: Option[String] = None,
    IsPhoneConfirmedSent: Option[Boolean] = None,
    PhoneConfirmedRegistrationSource: Option[String] = None,
    VerificationStatus: Option[String] = None,
    Avatar: Option[String] = None,
    BonusChosen: Option[String] = None,
    ZeroBounce: Option[String] = None,
    License: Option[String] = None,
    isZeroBalance: Option[String] = None,
    SportPreference: Option[String] = None,
    TotalBetsAmountSport: Option[String] = None,
    TotalBetsCountSport: Option[String] = None,
    AverageBetSizeSport: Option[String] = None,
    BonusToDepositRatioPercentage: Option[String] = None,
    TurnoverRatioCasino: Option[String] = None,
    TurnoverRatioSport: Option[String] = None,
    IsOptInCashback: Option[String] = None,
    IsOptInWeeklyReload: Option[String] = None,
    IsOptInWeekendReload: Option[String] = None,
    IsOptInForLastFourPeriodsCashback: Option[String] = None,
    IsOptInForLastFourPeriodsWeeklyReload: Option[String] = None,
    IsOptInForLastFourPeriodsWeekendReload: Option[String] = None,
    // achievement
    achievement_name: Option[String] = None,
    achievement_completed: Option[String] = None,
    // accountfrozen
    block_reason: Option[String] = None,
    is_blocked: Option[String] = Some("false"),
    // wallet
    last_deposit_datetime: Option[String] = None, // not sent
    failed_deposit_date: Option[String] = None,
    is_first_deposit: Option[Boolean] = None,
    // TODO later account_balance: Option[String] = None,
    // logins
    last_login_datetime: Option[String] = None, // not sent
    GlobalProductClassification: Option[String] = None,
    ProductClassification: Option[String] = None,
    FavoriteGameIds: Option[List[Option[String]]] = None,
    FavoriteGameNames: Option[List[Option[String]]] = None,
    GGRAmount: Option[String] = None, // not sent
    GGRAmountEUR: Option[String] = None, // not sent
    NGRAmount: Option[String] = None, // not sent
    NGRAmountEUR: Option[String] = None, // not sent
    Balance: Option[String] = None, // not sent
    BalanceEUR: Option[String] = None, // not sent
    BonusGGRRatio: Option[String] = None, // not sent
    BonusDepositRatio: Option[String] = None, // not sent
    NGRDepositRatio: Option[String] = None, // not sent
    BonusAbusing: Option[String] = None,
    VIPLevel: Option[String] = None,
    BlockedCasino: Option[String] = None,
    BlockedSport: Option[String] = None,
    CanBeContactedBySMS: Option[String] = None,
    CanBeContactedByEmail: Option[String] = None,
    CanBeContactedByPhoneOrDynamicMessage: Option[String] = None,
    CanReceivePromotions: Option[String] = None,
    VIPMaxLevel: Option[String] = None,
    ExternalAffiliateID: Option[String] = None,
    RegistrationStatus: Option[String] = None,
    WelcomeChosenBonus: Option[String] = None,
    SentFirstDepDatetime: Option[Boolean] = None,
    VariantExperimentVersion: Option[String] = None,
    wos: Option[String] = None,
    wos2: Option[String] = None
)

case class PlayerStoreSQS(
    firstName: Option[String] = None,
    lastName: Option[String] = None,
    emailAddress: Option[String] = None,
    mobileNumber: Option[String] = None,
    brandId: Option[String] = None,
    brandName: Option[String] = None,
    registrationDatetime: Option[String] = None,
    affiliateID: Option[String] = None,
    language: Option[String] = None,
    countryID: Option[String] = None,
    first_dep_datetime: Option[String] = None,
    dateOfBirth: Option[String] = None,
    isSuppressed: Option[String] = None,
    postcode: Option[String] = None,
    country: Option[String] = None,
    isSelfExcludedOtherBrand: Option[String] = None,
    accountType: Option[String] = None,
    bonusProgram: Option[String] = None,
    currency: Option[String] = None,
    duplicate: Option[String] = None,
    GDPR: Option[String] = None,
    gender: Option[String] = None,
    referralType: Option[String] = None,
    registrationPlatform: Option[String] = None,
    username: Option[String] = None,
    isEmailValid: Option[String] = None,
    isVerified: Option[String] = None,
    verificationstatus: Option[String] = None,
    avatar: Option[String] = None,
    bonusChosen: Option[String] = None,
    zeroBounce: Option[String] = None,
    license: Option[String] = None,
    isBlocked: Option[String] = Some("false"),
    blockReason: Option[String] = None,
    lastAchievementName: Option[String] = None,
    isSelfExcluded: Option[String] = None,
    isZeroBalance: Option[String] = None,
    firstDepositLoss: Option[String] = None,
    globalproductclassification: Option[String] = None,
    productclassification: Option[String] = None,
    vip: Option[String] = None,
    BonusAbusing: Option[String] = None,
    FavoriteGameIds: Option[String] = None,
    FavoriteGameNames: Option[String] = None,
    VIPLevel: Option[String] = None,
    BlockedCasino: Option[String] = None,
    BlockedSport: Option[String] = None,
    CanBeContactedBySMS: Option[String] = None,
    CanBeContactedByEmail: Option[String] = None,
    CanBeContactedByPhoneOrDynamicMessage: Option[String] = None,
    CanReceivePromotions: Option[String] = None,
    VIPMaxLevel: Option[String] = None,
    SportPreference: Option[String] = None,
    TotalBetsAmountSport: Option[String] = None,
    TotalBetsCountSport: Option[String] = None,
    AverageBetSizeSport: Option[String] = None,
    BonusToDepositRatioPercentage: Option[String] = None,
    TurnoverRatioCasino: Option[String] = None,
    TurnoverRatioSport: Option[String] = None,
    ExternalAffiliateID: Option[String] = None,
    IsOptInCashback: Option[String] = None,
    IsOptInWeeklyReload: Option[String] = None,
    IsOptInWeekendReload: Option[String] = None,
    IsOptInForLastFourPeriodsCashback: Option[String] = None,
    IsOptInForLastFourPeriodsWeeklyReload: Option[String] = None,
    IsOptInForLastFourPeriodsWeekendReload: Option[String] = None,
    RegistrationStatus: Option[String] = None,
    WelcomeChosenBonus: Option[String] = None,
    registrationSource: Option[String] = None,
    VariantExperimentVersion: Option[String] = None,
    wos: Option[String] = None,
    wos2: Option[String] = None
)

object PlayerStore {
  import io.circe.Encoder
  import io.circe.generic.semiauto._

  implicit val playerStoreEncoder: Encoder[PlayerStore] = deriveEncoder

}
object PlayerStoreSQS {

  import io.circe.Encoder
  import io.circe.generic.semiauto._

  implicit val playerStoreSQSEncoder: Encoder[PlayerStoreSQS] = deriveEncoder

  def fixBrandName(
      brandId: Option[String],
      brandName: Option[String]
  ): Option[String] = {
    (brandId, brandName) match {
      case (_, Some(x)) if x.toLowerCase() == "boomerangcasino" =>
        Some("boomerang")
      case (_, Some(x)) if x.toLowerCase() == "mrpachocasino" => Some("mrpacho")
      case (_, Some(x)) if x.toLowerCase() == "betman"        => Some("betmen")
      case (_, Some(x)) if x.toLowerCase() == "noabet"        => Some("naobet")
      case (Some("217"), _)                                   => Some("ingobet")
      case (_, y)                                             => y
    }
  }

  def apply(player: PlayerStore): PlayerStoreSQS = {
    new PlayerStoreSQS(
      firstName = player.first_name,
      lastName = player.last_name,
      emailAddress = player.email,
      mobileNumber = player.phone_number,
      brandId = player.brand_id,
      brandName =
        fixBrandName(player.brand_id, player.brand_name).map(_.toLowerCase()),
      affiliateID = player.affiliate_id,
      language = player.language,
      countryID = player.country_id,
      registrationDatetime = player.reg_datetime.map(keepYYYYMMDD(_)),
      first_dep_datetime = player.first_dep_datetime.map(keepYYYYMMDD(_)),
      dateOfBirth = player.dob.map(keepYYYYMMDD(_)),
      isSuppressed = player.is_suppressed,
      postcode = player.Postcode,
      country = player.Country,
      isSelfExcludedOtherBrand = player.IsSelfExcludedOtherBrand,
      accountType = player.AccountType,
      bonusProgram = player.BonusProgram,
      currency = player.Currency,
      duplicate = player.Duplicate,
      GDPR = player.GDPR,
      gender = player.Gender,
      referralType = player.ReferralType,
      registrationPlatform = player.RegistrationPlatform,
      username = player.Username,
      isEmailValid = player.IsEmailValid,
      isVerified = player.IsVerified,
      verificationstatus = player.VerificationStatus,
      avatar = player.Avatar,
      bonusChosen = player.BonusChosen,
      zeroBounce = player.ZeroBounce,
      license = player.License,
      isBlocked = player.is_blocked,
      blockReason = player.block_reason,
      lastAchievementName = player.achievement_name,
      isSelfExcluded = player.is_self_excluded,
      isZeroBalance = player.isZeroBalance,
      firstDepositLoss = player.isZeroBalance,
      globalproductclassification = player.GlobalProductClassification,
      productclassification = player.ProductClassification,
      vip = player.vip,
      BonusAbusing = player.BonusAbusing,
      VIPLevel = player.VIPLevel,
      BlockedCasino = player.BlockedCasino,
      BlockedSport = player.BlockedSport,
      CanBeContactedBySMS = player.CanBeContactedBySMS,
      CanBeContactedByEmail = player.CanBeContactedByEmail,
      CanBeContactedByPhoneOrDynamicMessage =
        player.CanBeContactedByPhoneOrDynamicMessage,
      CanReceivePromotions = player.CanReceivePromotions,
      FavoriteGameIds =
        player.FavoriteGameIds.map(f => f.flatten.mkString("|")),
      FavoriteGameNames =
        player.FavoriteGameNames.map(f => f.flatten.mkString("|")),
      VIPMaxLevel = player.VIPMaxLevel,
      SportPreference = player.SportPreference,
      TotalBetsAmountSport = player.TotalBetsAmountSport,
      TotalBetsCountSport = player.TotalBetsCountSport,
      AverageBetSizeSport = player.AverageBetSizeSport,
      BonusToDepositRatioPercentage = player.BonusToDepositRatioPercentage,
      TurnoverRatioCasino = player.TurnoverRatioCasino,
      TurnoverRatioSport = player.TurnoverRatioSport,
      ExternalAffiliateID = player.ExternalAffiliateID,
      IsOptInCashback = player.IsOptInCashback,
      IsOptInWeeklyReload = player.IsOptInWeeklyReload,
      IsOptInWeekendReload = player.IsOptInWeekendReload,
      IsOptInForLastFourPeriodsCashback =
        player.IsOptInForLastFourPeriodsCashback,
      IsOptInForLastFourPeriodsWeeklyReload =
        player.IsOptInForLastFourPeriodsWeeklyReload,
      IsOptInForLastFourPeriodsWeekendReload =
        player.IsOptInForLastFourPeriodsWeekendReload,
      RegistrationStatus = player.RegistrationStatus,
      WelcomeChosenBonus = player.WelcomeChosenBonus,
      registrationSource = player.registrationSource,
      VariantExperimentVersion = player.VariantExperimentVersion,
      wos = player.wos,
      wos2 = player.wos2
    )
  }
}
