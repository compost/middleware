package com.jada.models

case class Login(
    player_id: Option[String] = None,
    brand_id: Option[String] = None,
    login_success: Option[String] = None,
    login_datetime: Option[String] = None
)

case class LoginSQS(
    player_id: Option[String] = None,
    brand_id: Option[String] = None,
    login_success: Option[String] = None,
    login_date: Option[String] = None
)

object LoginSQS {

  def apply(login: Login): LoginSQS = {
    LoginSQS(
      player_id = login.player_id,
      brand_id = login.brand_id,
      login_success = login.login_success,
      login_date = login.login_datetime.map(d => if(d.length() >= 10) d.substring(0, 10) else d),
    )

  }
}
