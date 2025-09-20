# Existing Azure resources
name                 = "gig-environment"
client               = "GiG"
resource_group_name  = "GiC"
virtual_network_name = "GiC-vnet"
subnet_name          = "gig-environment"
address_prefixes     = "10.5.6.0/23"

# New resources to be created
storage_account_name = "gigenvstorage" # Must be globally unique
# Environmental variables for your application
#aws_access_key_id    = ""
#aws_secret_key_value = ""


# List of container apps to be created
container_apps = [
  {
    name               = "slotbox-main"
    cpu                = 1
    memory             = 2
    docker_image_name  = "slotbox/slotbox"
    docker_image_tag   = "20250920"
    storage_share_name = "sharestate"
    envs               = {}
    secrets            = {}
    aca                = true
  },
  {
    name   = "gig-main"
    cpu    = 2
    memory = 4

    docker_image_name  = "gig/gig-stream"
    docker_image_tag   = "20250825"
    storage_share_name = "gig-sharestate"
    envs = {
      "STATE_DIR" = "/app/state/2025-08-23"
    }
    secrets    = {}
    aca        = true
  }

]

# Map of storage configurations to be created
storage_configs = {
  "sharestate" = {
    quota = 50 # in GB
    aca   = true
  }
  "gig-sharestate" = {
    quota = 50 # in GB
    aca   = true
  }
}
