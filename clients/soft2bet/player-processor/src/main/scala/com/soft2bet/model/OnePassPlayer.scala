package com.soft2bet.model

import java.text.SimpleDateFormat

case class OnePassPlayer(
    player_id: Option[String],
    brand_id: Option[String],
    brand_name: Option[String],
    reg_datetime: Option[String],
    GDPR: Option[String],
    email: Option[String],
    phone_number: Option[String],
    Gender: Option[String],
    Country: Option[String],
    Currency: Option[String],
    language: Option[String],
    last_name: Option[String],
    Postcode: Option[String],
    first_name: Option[String],
    IsVerified: Option[String],
    AccountType: Option[String],
    dob: Option[String],
    LastLoginDate: Option[String]
)

case class OnePassPlayerStore(
    player_id: Option[String] = None,
    brand_id: Option[String] = None,
    brand_name: Option[String] = None,
    reg_datetime: Option[String] = None,
    GDPR: Option[String] = None,
    email: Option[String] = None,
    phone_number: Option[String] = None,
    Gender: Option[String] = None,
    Country: Option[String] = None,
    Currency: Option[String] = None,
    language: Option[String] = None,
    last_name: Option[String] = None,
    Postcode: Option[String] = None,
    first_name: Option[String] = None,
    IsVerified: Option[String] = None,
    AccountType: Option[String] = None,
    dob: Option[String] = None,
    LastLoginDate: Option[String] = None
)

case class OnePassPlayerRegisteredSQS(
    originalId: Option[String],
    brandId: Option[String],
    brandName: Option[String],
    reg_datetime: Option[String],
    gdpr: Option[String],
    emailAddress: Option[String],
    mobileNumber: Option[String],
    sex: Option[String],
    country: Option[String],
    currency: Option[String],
    language: Option[String],
    lastname: Option[String],
    zip: Option[String],
    firstname: Option[String],
    isverified: Option[String],
    accounttype: Option[String],
    dateofbirth: Option[String]
)

case class OnePassPlayerLoginSQS(
    originalId: Option[String],
    brandId: Option[String],
    lastlogindate: String
)

object OnePassPlayerStore {

  def apply(
      previous: Option[OnePassPlayerStore],
      player: OnePassPlayer
  ): OnePassPlayerStore = {
    previous match {
      case Some(p) =>
        p.copy(
          player_id = player.player_id.orElse(player.player_id),
          brand_id = player.brand_id.orElse(player.brand_id),
          brand_name = player.brand_name.orElse(player.brand_name),
          reg_datetime = player.reg_datetime.orElse(player.reg_datetime),
          GDPR = player.GDPR.orElse(player.GDPR),
          email = player.email.orElse(player.email),
          phone_number = player.phone_number.orElse(player.phone_number),
          Gender = player.Gender.orElse(player.Gender),
          Country = player.Country.orElse(player.Country),
          Currency = player.Currency.orElse(player.Currency),
          language = player.language.orElse(player.language),
          last_name = player.last_name.orElse(player.last_name),
          Postcode = player.Postcode.orElse(player.Postcode),
          first_name = player.first_name.orElse(player.first_name),
          IsVerified = player.IsVerified.orElse(player.IsVerified),
          AccountType = player.AccountType.orElse(player.AccountType),
          dob = player.dob.orElse(player.dob),
          LastLoginDate = player.LastLoginDate.orElse(player.LastLoginDate)
        )
      case None =>
        OnePassPlayerStore(
          player_id = player.player_id,
          brand_id = player.brand_id,
          brand_name = player.brand_name,
          reg_datetime = player.reg_datetime,
          GDPR = player.GDPR,
          email = player.email,
          phone_number = player.phone_number,
          Gender = player.Gender,
          Country = player.Country,
          Currency = player.Currency,
          language = player.language,
          last_name = player.last_name,
          Postcode = player.Postcode,
          first_name = player.first_name,
          IsVerified = player.IsVerified,
          AccountType = player.AccountType,
          dob = player.dob,
          LastLoginDate = player.LastLoginDate
        )
    }
  }
}

object OnePassPlayerRegisteredSQS {
  def apply(
      player: OnePassPlayerStore
  ): OnePassPlayerRegisteredSQS = {
    OnePassPlayerRegisteredSQS(
      originalId = player.player_id,
      brandId = player.brand_id,
      brandName = player.brand_name.map(_.toLowerCase()),
      reg_datetime = player.reg_datetime.map(v =>
        if (v.length > 10) v.substring(0, 10) else v
      ),
      gdpr = player.GDPR,
      emailAddress = player.email,
      mobileNumber = player.phone_number,
      sex = player.Gender,
      country = player.Country,
      currency = player.Currency,
      language = player.language,
      lastname = player.last_name,
      zip = player.Postcode,
      firstname = player.first_name,
      isverified = player.IsVerified,
      accounttype = player.AccountType,
      dateofbirth =
        player.dob.map(v => if (v.length > 10) v.substring(0, 10) else v)
    )
  }
}

object OnePassPlayerLoginSQS {
  val format = new SimpleDateFormat("yyyy-MM-dd")

  def getDate(): String = {
    format.format(new java.util.Date())
  }
  def apply(
      player: OnePassPlayerStore
  ): OnePassPlayerLoginSQS = {
    OnePassPlayerLoginSQS(
      originalId = player.player_id,
      brandId = player.brand_id,
      lastlogindate = player.LastLoginDate
        .map(v => if (v.length > 10) v.substring(0, 10) else v)
        .getOrElse(getDate())
    )
  }
}
