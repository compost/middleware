version_main:="20251021"

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
  @just stop-main 

deactivate-soft2bet-realtime:
  #@just deactivate Soft2Bet soft2bet-logins 
  @just deactivate Soft2Bet soft2bet-firstdepositloss
  @just deactivate Soft2Bet soft2bet-sportpush
  @just deactivate Soft2Bet soft2bet-repartitioner
  @just deactivate Soft2Bet soft2bet-funid
  @just stop-main 


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

start-main:
  az webapp start --name soft2bet-players-processor --resource-group Soft2Bet

stop-main:
  az webapp stop --name soft2bet-players-processor --resource-group Soft2Bet

deploy-main:
  az webapp config container set --resource-group Soft2Bet --name soft2bet-players-processor --container-image soft2bet.azurecr.io/player-processor:{{version_main}}

