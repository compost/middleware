# Existing Azure resources
name                 = "winner-studio-environment"
client               = "Winner-Studio"
resource_group_name  = "WinnerStudio-RG"
virtual_network_name = "WinnerStudio-vnet"
subnet_name          = "winner-studio-environment"
address_prefixes     = "10.18.6.0/23"

# New resources to be created
storage_account_name = "winnerstudiostorage" # Must be globally unique

# Environmental variables for your application
#aws_access_key_id    = ""
#aws_secret_key_value = ""


# List of container apps to be created
container_apps = [
  {
    name               = "winnerstudio-main"
    cpu                = 1
    memory             = 2
    docker_image_name  = "winner-studio/winner-studio-stream"
    docker_image_tag   = "20250825"
    storage_share_name = "sharestate"
    envs = {
      "WEBSITE_DNS_SERVER" : "168.63.129.16"
      "STATE_DIR" : "/app/state/20250825"
      "QUARKUS_DATASOURCE_ACQUISITION_TIMEOUT" : "180s"
    }
    secrets    = {}
    datasource = true
    aca        = true
  }
]

# Map of storage configurations to be created
storage_configs = {
  "sharestate" = {
    quota = 50 # in GB
    aca   = true
  }
}
