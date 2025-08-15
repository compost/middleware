#!/usr/bin/env fish

set apps (cat applications.json | jq -r '.app')

for app in $apps
   az webapp stop --name $app --resource-group Soft2Bet 
end

