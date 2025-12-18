# Existing Azure resources
name                 = "finnplay-environment"
client               = "Finnplay"
resource_group_name  = "Finnplay"
virtual_network_name = "Finnplay-vnet"
subnet_name          = "finnplay-environment"
address_prefixes     = "10.6.6.0/23"

container_registry_name         = "finplayacr"
resource_group_name_of_registry = "Finnplay"
# New resources to be created
storage_account_name = "finnplayenvstorage" # Must be globally unique

# Environmental variables for your application
#aws_access_key_id    = ""
#aws_secret_key_value = ""


# List of container apps to be created
container_apps = [
  {
    name               = "finnplay-main"
    cpu                = 2
    memory             = 4
    docker_image_name  = "finnplay/finnplay-stream"
    docker_image_tag   = "20251021"
    storage_share_name = "main-sharestate"
    envs = {
      "STATE_DIR"          = "/app/state/2025-08-26"
      "WEBSITE_DNS_SERVER" = "168.63.129.16"
    }
    secrets    = {}
    datasource = true
    aca        = true
  }

]

# Map of storage configurations to be created
storage_configs = {
  "main-sharestate" = {
    quota = 50 # in GB
    aca   = true
  }
}
