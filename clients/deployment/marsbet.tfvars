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
    name               = "marsbet-main"
    cpu                = 0.5
    memory             = 1.0
    docker_image_name  = "marsbet/base-stream"
    docker_image_tag   = "20250826"
    storage_share_name = "marsbet-sharestate"
    envs = {
      "STATE_DIR" = "/app/state/2025-08-26"
    }
    secrets    = {}
    aca        = true
  },

  {
    name               = "marsbet-mirror-main"
    cpu                = 1
    memory             = 2
    docker_image_name  = "kafka-mirror-group/kafka-mirror"
    docker_image_tag   = "hostage"
    storage_share_name = "mirror-main-sharestate"
    envs = {
      "MIRROR_CONSUMER_GROUP_ID" = "marsbet-mirror-main"
      "KAFKA_SOURCE_TOPIC" = "marsbet-v1-player-store-changelog"
      "KAFKA_TARGET_TOPIC" = "marsbet-v1-player-store-changelog"  
      "KAFKA_SOURCE_BOOTSTRAP_SERVERS" = "10.21.1.4:9092,10.21.1.5:9092,10.21.1.6:9092"
      "KAFKA_TARGET_BOOTSTRAP_SERVERS" = "10.21.3.14:9092,10.21.3.15:9092,10.21.3.16:9092"
    }
    secrets    = {}
    aca        = true
  }

]

# Map of storage configurations to be created
storage_configs = {
  "marsbet-sharestate" = {
    quota = 50 # in GB
    aca   = true
  }

  "mirror-main-sharestate" = {
    quota = 1 # in GB
    aca   = true
  }
}
