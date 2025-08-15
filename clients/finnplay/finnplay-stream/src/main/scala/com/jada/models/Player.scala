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
    real_country_id: Option[String] = None,
    vip: Option[String] = None,
    currency_id: Option[String] = None,
    user_validated: Option[String] = None,
    user_validated_time: Option[String] = None,
    receive_email: Option[String] = None,
    receive_sms: Option[String] = None,
    test_user: Option[String] = None,
    nick_name: Option[String] = None,
    gender_id: Option[String] = None,
    gender_description: Option[String] = None,
    address: Option[String] = None,
    zip_code: Option[String] = None,
    city: Option[String] = None,
    device_type_id: Option[String] = None,
    device_type_description: Option[String] = None,
    player_status_id: Option[String] = None,
    player_status_description: Option[String] = None,
    SITE_ID: Option[String] = None,
    Benefit: Option[String] = None,
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
    currency: Option[String] = None,
    country: Option[String] = None,
    user_validated: Option[String] = None,
    user_validated_time: Option[String] = None,
    site_id: Option[String] = None,
    last_login_datetime: Option[String] = None,
    test_user: Option[String] = None,
    nick_name: Option[String] = None,
    gender_id: Option[String] = None,
    gender_description: Option[String] = None,
    address: Option[String] = None,
    zip_code: Option[String] = None,
    city: Option[String] = None,
    receive_email: Option[String] = None,
    receive_sms: Option[String] = None,
    device_type_id: Option[String] = None,
    device_type_description: Option[String] = None,
    player_status_id: Option[String] = None,
    player_status_description: Option[String] = None,
    Benefit: Option[String] = None,
)

object PlayerSQS {

  def deserialize(data: Array[Byte]): PlayerSQS = {
    import io.circe._
    import io.circe.generic.auto._
    parser.decode[PlayerSQS](new String(data)).right.get
  }

  import java.time.format.DateTimeFormatter
  import java.time.ZoneId

  val kafkaFormat = DateTimeFormatter
    .ofPattern("yyyy-MM-dd' 'HH:mm:ss")
    .withZone(ZoneId.of("UTC"))
  val sqsFormat = DateTimeFormatter
    .ofPattern("yyyy-MM-dd' 'HH:mm:ss")
    .withZone(ZoneId.of("Europe/Budapest"))

  def keepDate(brandId: Option[String], d: String): String = {
    if (d.length() > 10) {
      val dd = brandId
        .filter(_ == "106")
        .map(_ => {
          val kafka = kafkaFormat.parse(d)
          sqsFormat.format(kafka)
        })
        .getOrElse(d)

      dd.substring(0, 10)
    } else {
      d
    }
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
      reg_datetime = player.reg_datetime.map(keepDate(player.brand_id, _)),
      first_name = player.first_name,
      last_name = player.last_name,
      email = player.email,
      phone_number = player.phone_number,
      language = player.language,
      affiliate_id = player.affiliate_id,
      is_self_excluded = player.is_self_excluded,
      first_dep_datetime =
        player.first_dep_datetime.map(keepDate(player.brand_id, _)),
      dob = player.dob.map(keepDate(player.brand_id, _)),
      country = com.jada.Countries(countryId),
      vip = player.vip,
      user_validated = player.user_validated,
      user_validated_time =
        player.user_validated_time.map(keepDate(player.brand_id, _)),
      currency = player.currency_id.map(v =>
        referentials.currencies
          .get(player.brand_id.getOrElse(""))
          .getOrElse(Map.empty[String, String])
          .get(v)
          .getOrElse("")
      ),
      country_id = countryId,
      site_id = player.SITE_ID,
      last_login_datetime =
        player.last_login_datetime.map(keepDate(player.brand_id, _)),
      test_user = player.test_user,
      nick_name = player.nick_name,
      gender_id = player.gender_id,
      gender_description = player.gender_description,
      address = player.address,
      zip_code = player.zip_code,
      city = player.city,
      receive_email = player.receive_email,
      receive_sms = player.receive_sms,
      device_type_id = player.device_type_id,
      device_type_description = player.device_type_description,
      player_status_id = player.player_status_id,
      player_status_description = player.player_status_description,
      Benefit = player.Benefit
    )
  }
}
