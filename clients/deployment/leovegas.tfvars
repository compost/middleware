# Existing Azure resources
name                 = "leovegas-environment"
client               = "leovegas"
resource_group_name  = "LeoVegas-RG"
virtual_network_name = "vnet-leovegas"
subnet_name          = "leovegas-environment"
address_prefixes     = "10.10.6.0/23"

# New resources to be created
storage_account_name = "leovegasenvstorage" # Must be globally unique

# Environmental variables for your application
#aws_access_key_id    = ""
#aws_secret_key_value = ""


# List of container apps to be created
container_apps = [
  {
    name               = "leovegas-eventhub"
    cpu                = 1.25
    memory             = 2.5
    docker_image_name  = "leovegas-trigger-journey"
    docker_image_tag   = "20250910"
    storage_share_name = "leovegas-eventhub-sharestate"
    envs = {
      "QUARKUS_SQS_AWS_REGION" = "eu-central-1"
      "STATE_DIR"          = "/app/state/current"
    }
    secrets    = {}
    datasource = false
    eventhub = true
    aca = true
  }

]

# Map of storage configurations to be created
storage_configs = {
  "leovegas-eventhub-sharestate" = {
    quota = 5 # in GB
    aca   = true
  }
}
