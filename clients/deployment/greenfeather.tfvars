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
    name               = "greenfeather-main"
    cpu                = 2
    memory             = 4
    docker_image_name  = "greenfeather/greenfeather-stream"
    docker_image_tag   = "20250824"
    storage_share_name = "main-sharestate"
    envs = {
      "PUNCTUATOR" = "PT60M"
      "STATE_DIR"  = "/app/state/2025-08-23"
    }
    secrets    = {}
    aca        = true
  },
  {
    name               = "greenfeather-batch"
    cpu                = 2
    memory             = 4
    docker_image_name  = "greenfeather-batch/greenfeather-stream-batch"
    docker_image_tag   = "20250824"
    storage_share_name = "batch-sharestate"
    envs = {
      "STATE_DIR" = "/app/state/2025-08-23"
    }
    secrets    = {}
    aca        = true
  },

  {
    name               = "greenfeather-mirror-main"
    cpu                = 0.5
    memory             = 1
    docker_image_name  = "kafka-mirror-group/kafka-mirror"
    docker_image_tag   = "anglet"
    storage_share_name = "mirror-main-sharestate"
    envs = {
      "MIRROR_CONSUMER_GROUP_ID" = "greenfeather-mirror-main"
      "KAFKA_SOURCE_TOPIC" = "greenfeather-player-store-changelog"
      "KAFKA_TARGET_TOPIC" = "greenfeather-player-store-changelog"  
      "KAFKA_SOURCE_BOOTSTRAP_SERVERS" = "10.20.1.4:9092,10.20.1.5:9092,10.20.1.6:9092"
      "KAFKA_TARGET_BOOTSTRAP_SERVERS" = "10.20.5.6:9092,10.20.5.7:9092,10.20.5.8:9092"
    }
    secrets    = {}
    aca        = true
  },

  {
    name               = "greenfeather-mirror-batch"
    cpu                = 0.5
    memory             = 1
    docker_image_name  = "kafka-mirror-group/kafka-mirror"
    docker_image_tag   = "anglet"
    storage_share_name = "mirror-batch-sharestate"
    envs = {
      "MIRROR_CONSUMER_GROUP_ID" = "greenfeather-mirror-batch"
      "KAFKA_SOURCE_TOPIC" = "greenfeather-batch-player-store-changelog"
      "KAFKA_TARGET_TOPIC" = "greenfeather-batch-player-store-changelog"  
      "KAFKA_SOURCE_BOOTSTRAP_SERVERS" = "10.20.1.4:9092,10.20.1.5:9092,10.20.1.6:9092"
      "KAFKA_TARGET_BOOTSTRAP_SERVERS" = "10.20.5.6:9092,10.20.5.7:9092,10.20.5.8:9092"
    }
    secrets    = {}
    aca        = true
  }
]

# Map of storage configurations to be created
storage_configs = {
  "main-sharestate" = {
    quota = 50 # in GB
    aca   = true
  },
  "batch-sharestate" = {
    quota = 50 # in GB
    aca   = true
  }
 "mirror-main-sharestate" = {
    quota = 1 # in GB
    aca   = true
  }
 "mirror-batch-sharestate" = {
    quota = 1 # in GB
    aca   = true
  }


}
