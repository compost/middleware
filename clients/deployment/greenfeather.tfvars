# Existing Azure resources
name                 = "greenfeather-environment"
client               = "GreenFeather"
resource_group_name  = "GreenFeature-RG"
virtual_network_name = "GreenFeature-vnet"
subnet_name          = "greenfeather-environment"
address_prefixes     = "10.20.6.0/23"

# New resources to be created
storage_account_name = "greenfeatherstorage" # Must be globally unique

# Environmental variables for your application
#aws_access_key_id    = ""
#aws_secret_key_value = ""


# List of container apps to be created
container_apps = [
  {
    name               = "greenfeather-joy-main"
    cpu                = 2
    memory             = 4
    docker_image_name  = "greenfeather/greenfeather-stream"
    docker_image_tag   = "20251027"
    storage_share_name = "joy-main-2-sharestate"
    envs = {
      "PUNCTUATOR" = "PT60M"
      "STATE_DIR"  = "/app/state/20251027"
    }
    secrets = {}
    aca     = true
  },
  {
    name               = "greenfeather-joy-batch"
    cpu                = 2
    memory             = 4
    docker_image_name  = "greenfeather-batch/greenfeather-stream-batch"
    docker_image_tag   = "20250925"
    storage_share_name = "joy-batch-sharestate"
    envs = {
      "STATE_DIR" = "/app/state/current"
    }
    secrets = {}
    aca     = true
  },


]

# Map of storage configurations to be created
storage_configs = {
  "joy-main-2-sharestate" = {
    quota = 50 # in GB
    aca   = true
  },
  "joy-batch-sharestate" = {
    quota = 50 # in GB
    aca   = true
  }
}
