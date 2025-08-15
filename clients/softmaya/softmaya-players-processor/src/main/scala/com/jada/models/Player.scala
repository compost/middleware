package com.jada.models

// {"player_id":"15470","brand_id":"73","reg_datetime":"2023-04-03T23:13:01.5337680Z","first_name":"Russ","last_name":"Haxell","email":"russhaxell@gmail.com","phone_number":"+61482911714","language":"English","affiliate_id":"1","is_self_excluded":"false","first_dep_datetime":"2023-04-03T23:16:06Z","dob":"1995-09-14","country_id":"1","vip":"false","test_user":"false","currency_id":"1"}
case class Player(
    player_id: Option[String] = None,
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
    country_id: Option[String] = None,
    vip: Option[String] = None,
    currency_id: Option[String] = None
)

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
    country_id: Option[String] = None,
    vip: Option[String] = None,
    currency: Option[String] = None
)

object PlayerSQS {

  def deserialize(data: Array[Byte]): PlayerSQS = {
    import io.circe._
    import io.circe.generic.auto._
    parser.decode[PlayerSQS](new String(data)).right.get
  }


  def keepYearMonthDay(date: String): String = {
    if (date.length > 10) {
      date.substring(0, 10)
    } else {
      date
    }
  }

  def apply(
      player: PlayerStore,
      referentials: com.jada.Referentials
  ): PlayerSQS = {
    new PlayerSQS(
      originalId = player.player_id,
      brand_id = player.brand_id,
      reg_datetime = player.reg_datetime.map(keepYearMonthDay(_)),
      first_name = player.first_name,
      last_name = player.last_name,
      email = player.email,
      phone_number = player.phone_number,
      language = player.language,
      affiliate_id = player.affiliate_id,
      is_self_excluded = player.is_self_excluded,
      first_dep_datetime =
        player.first_dep_datetime.map(PlayerSQS.keepYearMonthDay(_)),
      dob = player.dob,
      vip = player.vip,
      currency = player.currency_description,
      country_id = player.country_id.map(v =>
        referentials.countries
          .get(player.brand_id.getOrElse(""))
          .getOrElse(Map.empty[String, String])
          .get(v)
          .getOrElse("")
      )
    )

  }
}
