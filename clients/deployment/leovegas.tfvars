# Existing Azure resources
name                 = "leovegas-environment"
client               = "leovegas"
resource_group_name  = "LeoVegas-RG"
virtual_network_name = "vnet-leovegas"
subnet_name          = "leovegas-environment"
address_prefixes     = "10.10.9.0/27"

workload_profile = "Consumption"
# New resources to be created
storage_account_name = "leovegas2envstorage" # Must be globally unique
create_outbound_ip   = true
# Environmental variables for your application
#aws_access_key_id    = 
#aws_secret_key_value = ""


# List of container apps to be created
container_apps = [
  {
    name               = "leovegas-eventhub"
    cpu                = 0.5
    memory             = 1
    docker_image_name  = "leovegas-trigger-journey"
    docker_image_tag   = "202509100020"
    storage_share_name = "leovegas-eventhub-sharestate"
    envs = {
      "QUARKUS_SQS_AWS_REGION" = "eu-central-1"
      "STATE_DIR"              = "/app/state/current"
    }
    secrets  = {}
    eventhub = true
    aca      = true
  },

  {
    name               = "leovegas-mks"
    cpu                = 1
    memory             = 2
    docker_image_name  = "leovegas/leovegas"
    docker_image_tag   = "20251125"
    storage_share_name = "leovegas-mks"
    envs = {
      "STATE_DIR" = "/app/state/20251029"
    }
    secrets = {}
    mks     = true
    aca     = true
  }

]

# Map of storage configurations to be created
storage_configs = {
  "leovegas-eventhub-sharestate" = {
    quota = 5 # in GB
    aca   = true
  }
  "leovegas-mks" = {
    quota = 50 # in GB
    aca   = true
  }

}
