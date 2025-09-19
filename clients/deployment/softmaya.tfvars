# Existing Azure resources
name                 = "softmaya-environment"
client               = "softmaya"
resource_group_name  = "SoftMaya-RG"
virtual_network_name = "softmaya-vnet"
subnet_name          = "softmaya-environment"
address_prefixes     = "10.16.4.0/23"

# New resources to be created
storage_account_name = "softmayaenvstorage" # Must be globally unique

# Environmental variables for your application
#aws_access_key_id    = ""
#aws_secret_key_value = ""


# List of container apps to be created
container_apps = [
  {
    name               = "softmaya-main"
    cpu                = 1
    memory             = 2
    docker_image_name  = "softmaya-players-processor"
    docker_image_tag   = "20250827"
    storage_share_name = "softmaya-sharestate"
    envs = {
      "STATE_DIR" = "/app/state/2025-08-26"
    }
    secrets    = {}
    aca        = true
  }

]

# Map of storage configurations to be created
storage_configs = {
  "softmaya-sharestate" = {
    quota = 50 # in GB
    aca   = true
  }
}
