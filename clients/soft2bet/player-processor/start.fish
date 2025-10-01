#!/usr/bin/env fish

set infos (cat applications.json | jq -r '.app + " " + .host')

for info in $infos
    set app $(echo $info | choose 0) 
    set host $(echo $info | choose 1) 
    az webapp start --name $app --resource-group Soft2Bet 
end

