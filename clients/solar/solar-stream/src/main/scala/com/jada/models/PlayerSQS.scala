package com.jada.models

case class PlayerSQS(
    originalId: Option[String] = None,
    reg_date: Option[String] = None,
    brand_id: Option[String] = None,
    brand_name: Option[String] = None,
    firstName: Option[String] = None,
    lastName: Option[String] = None,
    emailAddress: Option[String] = None,
    mobileNumber: Option[String] = None,
    language: Option[String] = None,
    affiliate_id: Option[String] = None,
    is_self_excluded: Option[String] = None,
    first_dep_datetime: Option[String] = None,
    dateOfBirth: Option[String] = None,
    country_id: Option[String] = None,
    country: Option[String] = None,
    vip: Option[String] = None,
    vip_level: Option[String] = None,
    currency_id: Option[String] = None,
    currency: Option[String] = None,
    test_user: Option[String] = None,
    consent_email: Option[String] = None,
    consent_sms: Option[String] = None
)

object PlayerSQS {

  def deserialize(data: Array[Byte]): PlayerSQS = {
    import io.circe._
    import io.circe.generic.auto._
    parser.decode[PlayerSQS](new String(data)).right.get
  }

  def apply(
      player: PlayerStore,
      referentials: com.jada.Referentials
  ): PlayerSQS = {
    val countryId = player.country_id.map(v =>
      referentials.countries
        .get(player.brand_id.getOrElse(""))
        .getOrElse(Map.empty[String, String])
        .get(v)
        .getOrElse("")
    )
    new PlayerSQS(
      originalId = player.player_id,
      brand_id = player.brand_id,
      brand_name = player.brand_name,
      reg_date = player.reg_datetime.map(d => d.replace("T", " ")),
      firstName = player.first_name,
      lastName = player.last_name,
      emailAddress = player.email,
      mobileNumber = player.phone_number,
      language = player.language,
      affiliate_id = player.affiliate_id,
      is_self_excluded = player.is_self_excluded,
      first_dep_datetime =
        player.first_dep_datetime.map(d => d.replace("T", " ")),
      dateOfBirth = player.dob,
      country = com.jada.Countries(countryId),
      vip = player.vip,
      vip_level = player.vip_level,
      currency = player.currency_id.map(v =>
        referentials.currencies
          .get(player.brand_id.getOrElse(""))
          .getOrElse(Map.empty[String, String])
          .get(v)
          .getOrElse("")
      ),
      country_id = countryId,
      test_user = player.test_user,
      consent_sms = player.consent_sms,
      consent_email = player.consent_email
    )
  }
}
