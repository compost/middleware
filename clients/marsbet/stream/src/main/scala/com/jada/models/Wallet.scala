package com.jada.models

//  {"transaction_id":"835083","brand_id":"73","transaction_type_id":"Deposit","transaction_status_id":"Accepted","amount":"30","transaction_datetime":"2023-06-03T05:54:06.0283537Z","player_id":"29763","currency_id":"1" }
case class Wallet(
    player_id: Option[String] = None,
    brand_id: Option[String] = None,
    transaction_type_id: Option[String] = None,
    transaction_status_id: Option[String] = None,
    transaction_datetime: Option[String] = None,
    paymentMethod: Option[String] = None,
    amount_cur: Option[String] = None,
    currency_description: Option[String] = None
)

case class FailedWithdrawalSQS(
    player_id: Option[String] = None,
    brand_id: Option[String] = None,
    Last_failed_withdrawal_date: Option[String] = None,
    Last_payment_method: Option[String] = None,
    Last_Transaction_Amount: Option[String] = None,
    currency: Option[String] = None
)

object FailedWithdrawalSQS {
  def apply(
      wallet: Wallet
  ): FailedWithdrawalSQS = {
    new FailedWithdrawalSQS(
      player_id = wallet.player_id,
      brand_id = wallet.brand_id,
      Last_failed_withdrawal_date = wallet.transaction_datetime.map(v =>
        if (v.length() > 10) { v.substring(0, 10) }
        else { v }
      ),
      Last_payment_method = wallet.paymentMethod,
      Last_Transaction_Amount = wallet.amount_cur,
      currency = wallet.currency_description
    )
  }
}

case class FailedDepositSQS(
    player_id: Option[String] = None,
    brand_id: Option[String] = None,
    Last_failed_deposit_date: Option[String] = None,
    Last_payment_method: Option[String] = None,
    Last_Transaction_Amount: Option[String] = None,
    currency: Option[String] = None
)

object FailedDepositSQS {
  def apply(
      wallet: Wallet
  ): FailedDepositSQS = {
    new FailedDepositSQS(
      player_id = wallet.player_id,
      brand_id = wallet.brand_id,
      Last_failed_deposit_date = wallet.transaction_datetime.map(v =>
        if (v.length() > 10) { v.substring(0, 10) }
        else { v }
      ),
      Last_payment_method = wallet.paymentMethod,
      Last_Transaction_Amount = wallet.amount_cur,
      currency = wallet.currency_description
    )
  }
}

case class SuccessWithdrawalSQS(
    player_id: Option[String] = None,
    brand_id: Option[String] = None,
    Last_success_withdrawal_date: Option[String] = None,
    Last_payment_method: Option[String] = None,
    Last_Transaction_Amount: Option[String] = None,
    currency: Option[String] = None
)

object SuccessWithdrawalSQS {
  def apply(
      wallet: Wallet
  ): SuccessWithdrawalSQS = {
    new SuccessWithdrawalSQS(
      player_id = wallet.player_id,
      brand_id = wallet.brand_id,
      Last_success_withdrawal_date = wallet.transaction_datetime.map(v =>
        if (v.length() > 10) { v.substring(0, 10) }
        else { v }
      ),
      Last_payment_method = wallet.paymentMethod,
      Last_Transaction_Amount = wallet.amount_cur,
      currency = wallet.currency_description
    )
  }
}

case class SuccessDepositSQS(
    player_id: Option[String] = None,
    brand_id: Option[String] = None,
    Last_success_deposit_date: Option[String] = None,
    Last_payment_method: Option[String] = None,
    Last_Transaction_Amount: Option[String] = None,
    currency: Option[String] = None
)

object SuccessDepositSQS {
  def apply(
      wallet: Wallet
  ): SuccessDepositSQS = {
    new SuccessDepositSQS(
      player_id = wallet.player_id,
      brand_id = wallet.brand_id,
      Last_success_deposit_date = wallet.transaction_datetime.map(v =>
        if (v.length() > 10) { v.substring(0, 10) }
        else { v }
      ),
      Last_payment_method = wallet.paymentMethod,
      Last_Transaction_Amount = wallet.amount_cur,
      currency = wallet.currency_description
    )
  }
}
