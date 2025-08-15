#!/usr/bin/env fish

set infos (cat applications.json | jq -r '.app + ":" + .image')
set image_version $argv[1]

for info in $infos
  set app $(echo $info | choose -f ":" 0)
  set image $(echo $info | choose -f ":" 1)

  az webapp config container set --resource-group Soft2bet --name $app --container-image "$image:$image_version"  

end

