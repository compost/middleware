package com.jada.models
import com.jada.processor.MappingTransformer.toBoolean
import java.util.Locale

case class Player(
    player_id: Option[String] = None,
    reg_datetime: Option[String] = None,
    first_name: Option[String] = None,
    last_name: Option[String] = None,
    email: Option[String] = None,
    phone_number: Option[String] = None,
    language: Option[String] = None,
    affiliate_id: Option[String] = None,
    first_dep_datetime: Option[String] = None,
    dob: Option[String] = None,
    country_id: Option[String] = None,
    country_description: Option[String] = None,
    VIP: Option[String] = None,
    test_user: Option[String] = None,
    currency_id: Option[String] = None,
    currency_description: Option[String] = None,
    brand_id: Option[String] = None
)

case class PlayerSQS(
    originalId: Option[String] = None,
    player_id: Option[String] = None,
    reg_datetime: Option[String] = None,
    first_name: Option[String] = None,
    last_name: Option[String] = None,
    email: Option[String] = None,
    phone_number: Option[String] = None,
    language: Option[String] = None,
    affiliate_id: Option[String] = None,
    first_dep_datetime: Option[String] = None,
    dob: Option[String] = None,
    country: Option[String] = None,
    vip: Option[String] = None,
    test_user: Option[String] = None,
    currency: Option[String] = None,
    brand_id: Option[String] = None
)

case class PlayerDatabase(
    BRAND: Option[String] = None,
    IS_SELF_EXCLUDED: Option[String] = None,

    PLAYER_ID: Option[String] = None,
    BRAND_ID: Option[String] = None,
    AFFILIATE_ID: Option[String] = None,
    COUNTRY: Option[String] = None,
    CURRENCY: Option[String] = None,
    DOB: Option[String] = None,
    EMAIL: Option[String] = None,
    FIRST_DEP_DATETIME: Option[String] = None,
    FIRST_NAME: Option[String] = None,
    LANGUAGE: Option[String] = None,
    LAST_NAME: Option[String] = None,
    PHONE_NUMBER: Option[String] = None,
    REG_DATETIME: Option[String] = None,
    TEST_USER: Option[String] = None,
    VIP: Option[String] = None
)

object PlayerSQS {

  def deserialize(data: Array[Byte]): PlayerSQS = {
    import io.circe._
    import io.circe.generic.auto._
    parser.decode[PlayerSQS](new String(data)).right.get
  }

  def apply(
      player: PlayerStore
  ): PlayerSQS = {

    new PlayerSQS(
      originalId = player.player_id,
      player_id = player.player_id,
      brand_id = player.brand_id,
      reg_datetime = player.reg_datetime.map(v =>
        if (v.length() > 10) { v.substring(0, 10) }
        else { v }
      ),
      first_name = player.first_name,
      last_name = player.last_name,
      email = player.email,
      phone_number = player.phone_number,
      language = player.language,
      affiliate_id = player.affiliate_id,
      first_dep_datetime = player.first_dep_datetime.map(v =>
        if (v.length() > 10) { v.substring(0, 10) }
        else { v }
      ),
      dob = player.dob.map(v =>
        if (v.length() > 10) { v.substring(0, 10) }
        else { v }
      ),
      vip = player.VIP,
      test_user = player.test_user,
      currency = player.currency_description,
      country = player.country_description
    )
  }
}
