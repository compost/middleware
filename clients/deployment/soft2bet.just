version_main:="20251218"

soft2bet-deactivate-all:
  @just deactivate Soft2Bet soft2bet-logins 
  @just deactivate Soft2Bet soft2bet-segmentation 
  @just deactivate Soft2Bet soft2bet-deprecated-kpi
  @just deactivate Soft2Bet soft2bet-firstdepositloss
  @just deactivate Soft2Bet soft2bet-statistics
  @just deactivate Soft2Bet soft2bet-sportpush
  @just deactivate Soft2Bet soft2bet-ldc
  @just deactivate Soft2Bet soft2bet-repartitioner
  @just deactivate Soft2Bet soft2bet-funid
  @just deactivate Soft2Bet soft2bet-viplevel
  @just soft2bet-stop-main

soft2bet-deactivate-realtime:
  @just deactivate Soft2Bet soft2bet-logins 
  @just deactivate Soft2Bet soft2bet-firstdepositloss
  @just deactivate Soft2Bet soft2bet-sportpush
  @just deactivate Soft2Bet soft2bet-repartitioner
  @just deactivate Soft2Bet soft2bet-funid
  @just deactivate Soft2Bet soft2bet-viplevel
  @just soft2bet-stop-main 


soft2bet-deactivate-batch:
  @just deactivate Soft2Bet soft2bet-segmentation 
  @just deactivate Soft2Bet soft2bet-deprecated-kpi
  @just deactivate Soft2Bet soft2bet-statistics
  @just deactivate Soft2Bet soft2bet-ldc

soft2bet-activate-batch:
  @just activate Soft2Bet soft2bet-segmentation 
  @just activate Soft2Bet soft2bet-deprecated-kpi
  @just activate Soft2Bet soft2bet-statistics
  @just activate Soft2Bet soft2bet-ldc

soft2bet-start-main:
  az webapp start --name soft2bet-players-processor --resource-group Soft2Bet

soft2bet-stop-main:
  az webapp stop --name soft2bet-players-processor --resource-group Soft2Bet

soft2bet-deploy-main:
  az webapp config container set --resource-group Soft2Bet --name soft2bet-players-processor --container-image soft2bet.azurecr.io/player-processor:{{version_main}}


soft2bet-deactivate-checker:
  @just deactivate Soft2Bet soft2bet-checker-anglet 
  @just deactivate Soft2Bet soft2bet-checker-biarritz 

