deactivate-soft2bet:
  @just deactivate Soft2Bet soft2bet-logins 
  @just deactivate Soft2Bet soft2bet-segmentation 
  @just deactivate Soft2Bet soft2bet-deprecated-kpi
  @just deactivate Soft2Bet soft2bet-firstdepositloss
  @just deactivate Soft2Bet soft2bet-statistics
  @just deactivate Soft2Bet soft2bet-sportpush
  @just deactivate Soft2Bet soft2bet-ldc
  @just deactivate Soft2Bet soft2bet-repartitioner
  @just deactivate Soft2Bet soft2bet-funid

deactivate-soft2bet-batch:
  @just deactivate Soft2Bet soft2bet-segmentation 
  @just deactivate Soft2Bet soft2bet-deprecated-kpi
  @just deactivate Soft2Bet soft2bet-statistics
  @just deactivate Soft2Bet soft2bet-ldc

activate-soft2bet-batch:
  @just activate Soft2Bet soft2bet-segmentation 
  @just activate Soft2Bet soft2bet-deprecated-kpi
  @just activate Soft2Bet soft2bet-statistics
  @just activate Soft2Bet soft2bet-ldc
