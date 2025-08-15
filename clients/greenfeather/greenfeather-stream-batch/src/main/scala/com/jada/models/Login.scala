package com.jada.models

import scala.util.Try

case class Login(
    player_id: Option[String] = None,
    brand_id: Option[String] = None,
    login_datetime: Option[String] = None, 
    login_success: Option[String] = None
)

case class LoginSQS(
    originalId: Option[String] = None,
    brand_id: Option[String] = None,
    login_datetime: Option[String] = None, 
    login_success: Option[String] = None
)

object LoginSQS {

  def apply(player: Login): LoginSQS = {
    new LoginSQS(
      originalId = player.player_id,
      brand_id = player.brand_id,
      login_datetime = player.login_datetime.map(x => Try(x.substring(0,10)).getOrElse(x)),
      login_success = player.login_success
    )
  }
}
