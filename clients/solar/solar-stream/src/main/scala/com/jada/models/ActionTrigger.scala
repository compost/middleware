package com.jada.models

case class ActionTrigger(
    action: Option[String] = None,
    action_value: Option[String] = None,
    action_timestamp: Option[String] = None,
    player_id: Option[String] = None,
    brand_id: Option[String] = None
)

case class WithdrawSuccessSQS(
    originalId: String,
    brand_id: String,
    withdrawalStatus: String
)

object WithdrawSuccessSQS {
  def apply(at: ActionTrigger): WithdrawSuccessSQS = {
    // filter in topology for player_id and brand and action_value is tested in transformer
    new WithdrawSuccessSQS(
      at.player_id.get,
      at.brand_id.get,
      at.action_value.get
    )
  }
}

case class EmailVerificationSQS(
    originalId: String,
    brand_id: String,
    email_verification_url: String,
    emailVerificationStatus: String = "TRUE"
)

object EmailVerificationSQS {
  def apply(at: ActionTrigger): EmailVerificationSQS = {
    // filter in topology for player_id and brand and action_value is tested in transformer
    new EmailVerificationSQS(
      at.player_id.get,
      at.brand_id.get,
      at.action_value.get
    )
  }
}

case class DepositSQS(
    originalId: String,
    brand_id: String,
    lastDepositDate: String
)

object DepositSQS {
  def apply(at: ActionTrigger): DepositSQS = {
    // TODO action_timestamp check
    // filter in topology for player_id and brand and action_value is tested in transformer
    new DepositSQS(
      at.player_id.get,
      at.brand_id.get,
      at.action_timestamp.get
    )
  }
}

case class ForgotPasswordSQS(
    originalId: String,
    brand_id: String,
    forget_password_url: String
)
object ForgotPasswordSQS {
  def apply(at: ActionTrigger): ForgotPasswordSQS = {
    // filter in topology for player_id and brand and action_value is tested in transformer
    new ForgotPasswordSQS(
      at.player_id.get,
      at.brand_id.get,
      at.action_value.get
    )
  }
}
