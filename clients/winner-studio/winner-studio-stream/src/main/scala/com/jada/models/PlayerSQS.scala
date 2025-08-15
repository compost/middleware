package com.jada.models

case class PlayerSQS(
    originalId: Option[String] = None,
    brand_id: Option[String] = None,
    reg_datetime: Option[String] = None,
    first_name: Option[String] = None,
    last_name: Option[String] = None,
    email: Option[String] = None,
    phone_number: Option[String] = None,
    language: Option[String] = None,
    affiliate_id: Option[String] = None,
    is_self_excluded: Option[String] = None,
    first_dep_datetime: Option[String] = None,
    dob: Option[String] = None,
    vip: Option[String] = None,
    test_user: Option[String] = None,
    country_id: Option[String] = None,
    country: Option[String] = None,
    currency: Option[String] = None,
    app_push_consented: Option[String] = None,
    platform: Option[String] = None,
    timezone: Option[String] = None
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
      reg_datetime = player.reg_datetime.map(d => d.substring(0, 10)),
      first_name = player.first_name,
      last_name = player.last_name,
      email = player.email,
      phone_number = player.phone_number,
      language = player.language,
      affiliate_id = player.affiliate_id,
      is_self_excluded = player.is_self_excluded,
      first_dep_datetime =
        player.first_dep_datetime.map(d => d.substring(0, 10)),
      dob = player.dob.map(d => d.substring(0, 10)),
      country_id = player.country_id,
      country = com.jada.Countries(countryId),
      currency = player.currency_id.map(v =>
        referentials.currencies
          .get(player.brand_id.getOrElse(""))
          .getOrElse(Map.empty[String, String])
          .get(v)
          .getOrElse("")
      ),
      vip = player.vip,
      test_user = player.test_user,
      timezone = player.timezone,
      app_push_consented = player.app_push_consented,
      platform = player.platform
    )
  }
}
