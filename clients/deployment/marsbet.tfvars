# Existing Azure resources
name                 = "wisegaming-environment"
client               = "WiseGaming"
resource_group_name  = "WiseGaming-RG"
virtual_network_name = "WiseGaming-vnet"
subnet_name          = "wisegaming-environment"
address_prefixes     = "10.21.4.0/23"

# New resources to be created
storage_account_name = "wisegamingenvstorage" # Must be globally unique

# Environmental variables for your application
#aws_access_key_id    = ""
#aws_secret_key_value = ""


# List of container apps to be created
container_apps = [
  {
    name               = "marsbet-joy-main"
    cpu                = 1
    memory             = 2 
    docker_image_name  = "marsbet/base-stream"
    docker_image_tag   = "20250927"
    storage_share_name = "marsbet-joy-sharestate"
    envs = {
      "STATE_DIR" = "/app/state/current"
    }
    secrets    = {}
    aca        = true
  },
]

# Map of storage configurations to be created
storage_configs = {
  "marsbet-joy-sharestate" = {
    quota = 50 # in gb
    aca   = true
  }

}
