# Existing Azure resources
name                 = "soft2bet-environment"
client               = "soft2bet"
resource_group_name  = "Soft2Bet"
virtual_network_name = "Soft2Bet-vnet"
subnet_name          = "soft2bet-environment"
address_prefixes     = "10.4.8.0/23"

container_registry_name         = "soft2bet"
resource_group_name_of_registry = "Soft2Bet"
#
# New resources to be created
storage_account_name = "soft2betenvstorage" # Must be globally unique

# Environmental variables for your application
#aws_access_key_id    = ""
#aws_secret_key_value = ""


# List of container apps to be created
container_apps = [
  {
    name               = "logins"
    cpu                = 0.5
    memory             = 1
    docker_image_name  = "login/player-processor"
    docker_image_tag   = "202509142258"
    storage_share_name = "logins-sharestate"
    envs = {
      "STATE_DIR"          = "/app/state/current"
    }
    secrets    = {}
    datasource = false
    eventhub = true
    aca = true
  },

  {
    name               = "soft2bet-segmentation"
    cpu                = 1
    memory             = 2
    docker_image_name  = "segmentation/player-processor"
    docker_image_tag   = "202509142258"
    storage_share_name = "segmentation-sharestate"
    envs = {
      "STATE_DIR"          = "/app/state/current"
    }
    secrets    = {}
    datasource = false
    eventhub = false 
    aca = true
  }


]

# Map of storage configurations to be created
storage_configs = {
  "logins-sharestate" = {
    quota = 5 # in GB
    aca   = true
  }

  "segmentation-sharestate" = {
    quota = 50 # in GB
    aca   = true
  }
}
