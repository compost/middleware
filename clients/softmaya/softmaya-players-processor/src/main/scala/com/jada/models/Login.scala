package com.jada.models

case class Login(
    player_id: Option[String] = None,
    brand_id: Option[String] = None,
    login_datetime: Option[String] = None, 
    login_success: Option[String] = None, 
)

case class LoginSQS(
    originalId: Option[String] = None,
    brand_id: Option[String] = None,
    login_datetime: Option[String] = None, 
    login_success: Option[String] = None, 
)

object LoginSQS {

  def deserialize(data: Array[Byte]): LoginSQS = {
    import io.circe._
    import io.circe.generic.auto._
    parser.decode[LoginSQS](new String(data)).right.get
  }

  def apply(player: Login): LoginSQS = {
    new LoginSQS(
      originalId = player.player_id,
      brand_id = player.brand_id,
      login_datetime = player.login_datetime.map(PlayerSQS.keepYearMonthDay(_)),
      login_success = player.login_success
    )
  }
}
