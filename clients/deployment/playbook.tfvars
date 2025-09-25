# Existing Azure resources
name                 = "playbook-environment"
client               = "PlaybookEngineering"
resource_group_name  = "PlaybookEngineering-RG"
virtual_network_name = "PlaybookEngineering-Vnet"
subnet_name          = "playbook-environment"
address_prefixes     = "10.11.4.0/23"

# New resources to be created
storage_account_name = "playbookenvstorage" # Must be globally unique

# Environmental variables for your application
#aws_access_key_id    = ""
#aws_secret_key_value = ""


# List of container apps to be created
container_apps = [
  {
    name               = "playbook-main"
    cpu                = 0.5
    memory             = 1
    docker_image_name  = "playbook/playbook-stream"
    docker_image_tag   = "20250826"
    storage_share_name = "playbook-sharestate"
    envs = {
      "STATE_DIR"          = "/app/state/2025-08-26"
      "WEBSITE_DNS_SERVER" = "168.63.129.16"
    }
    secrets    = {}

    aca = true
  },

  {
    name               = "playbook-mirror-main"
    cpu                = 1
    memory             = 2
    docker_image_name  = "kafka-mirror-group/kafka-mirror"
    docker_image_tag   = "hostage"
    storage_share_name = "mirror-main-sharestate"
    envs = {
      "MIRROR_CONSUMER_GROUP_ID" = "playbook-mirror-main"
      "KAFKA_SOURCE_TOPIC" = "playbook-player-store-changelog"
      "KAFKA_TARGET_TOPIC" = "playbook-player-store-changelog"  
      "KAFKA_SOURCE_BOOTSTRAP_SERVERS" = "10.11.1.12:9092,10.11.1.13:9092,10.11.1.14:9092"
      "KAFKA_TARGET_BOOTSTRAP_SERVERS" = "10.11.6.20:9092,10.11.6.21:9092,10.11.6.22:9092"
    }
    secrets    = {}
    aca        = true
  },



]

# Map of storage configurations to be created
storage_configs = {
  "playbook-sharestate" = {
    quota = 50 # in GB
    aca   = true
  }

  "mirror-main-sharestate" = {
    quota = 1 # in GB
    aca   = true
  }
}
