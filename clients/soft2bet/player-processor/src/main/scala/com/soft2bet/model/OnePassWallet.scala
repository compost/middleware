package com.soft2bet.model

case class OnePassWallet(
    player_id: Option[String],
    brand_id: Option[String],
    currency: Option[String],
    PayMethod: Option[String],
    transaction_id: Option[String],
    transaction_datetime: Option[String],
    transaction_type_id: Option[String],
    amount: Option[String],
    transaction_status_id: Option[String]
)

case class OnePassWalletSQS(
    playerid: Option[String],
    brandId: Option[String],
    currency: Option[String],
    paymethod: Option[String],
    transactionid: Option[String],
    transactiondate: Option[String],
    transactiontype: Option[String],
    transactionamount: Option[String],
    transactionstatus: Option[String]
)

object OnePassWalletSQS {
  def apply(
      wallet: OnePassWallet
  ): OnePassWalletSQS = {
    OnePassWalletSQS(
      playerid = wallet.player_id,
      brandId = wallet.brand_id,
      currency = wallet.currency,
      paymethod = wallet.PayMethod,
      transactionid = wallet.transaction_id,
      transactiondate = wallet.transaction_datetime.map(v =>
        if (v.length > 10) v.substring(0, 10) else v
      ),
      transactiontype = wallet.transaction_type_id,
      transactionamount = wallet.amount,
      transactionstatus = wallet.transaction_status_id
    )
  }
}
