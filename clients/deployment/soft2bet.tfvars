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
    name               = "soft2bet-logins"
    cpu                = 0.5
    memory             = 1
    docker_image_name  = "login/player-processor"
    docker_image_tag   = "20251019"
    storage_share_name = "logins-sharestate"
    envs = {
      "STATE_DIR" = "/app/state/current"
    }
    secrets  = {}
    eventhub = true
    aca      = true
  },

  {
    name               = "soft2bet-segmentation"
    cpu                = 1
    memory             = 2
    docker_image_name  = "segmentation/player-processor"
    docker_image_tag   = "20251014"
    storage_share_name = "segmentation-sharestate"
    envs = {
      "STATE_DIR" = "/app/state/current"
    }
    secrets = {}
    aca     = true
  },

  {
    name               = "soft2bet-deprecated-kpi"
    cpu                = 0.5
    memory             = 1
    docker_image_name  = "kpi/player-processor"
    docker_image_tag   = "20251014"
    storage_share_name = "deprecated-kpi-sharestate"
    envs = {
      "STATE_DIR" = "/app/state/current"
    }
    secrets = {}
    aca     = true
  },


  {
    name               = "soft2bet-firstdepositloss"
    cpu                = 0.5
    memory             = 1
    docker_image_name  = "fld/player-processor"
    docker_image_tag   = "20251014"
    storage_share_name = "firstdepositloss-sharestate"
    envs = {
      "STATE_DIR" = "/app/state/current"
    }
    secrets = {}
    aca     = true
  },


  {
    name               = "soft2bet-statistics"
    cpu                = 0.5
    memory             = 1
    docker_image_name  = "statistics/player-processor"
    docker_image_tag   = "20251014"
    storage_share_name = "statistics-sharestate"
    envs = {
      "STATE_DIR" = "/app/state/current"
    }
    secrets = {}
    aca     = true
  },

  {
    name               = "soft2bet-sportpush"
    cpu                = 0.5
    memory             = 1
    docker_image_name  = "sportpush/player-processor"
    docker_image_tag   = "20251014"
    storage_share_name = "sportpush-sharestate"
    envs = {
      "STATE_DIR" = "/app/state/current"
    }
    secrets = {}
    aca     = true
  },


  {
    name               = "soft2bet-ldc"
    cpu                = 1
    memory             = 2
    docker_image_name  = "player-processor"
    docker_image_tag   = "20251014"
    storage_share_name = "ldc-2-sharestate"
    envs = {
      "STATE_DIR"                             = "/app/state/ldc-20251004"
      "DEFAULT_TOPOLOGY_ENABLED"              = "false"
      "LIFETIMEDEPOSITCOUNT_TOPOLOGY_ENABLED" = "true"
    }
    secrets = {}
    aca     = true
  },


  {
    name               = "soft2bet-repartitioner"
    cpu                = 1
    memory             = 2
    docker_image_name  = "repartitioner/player-processor"
    docker_image_tag   = "20251014"
    storage_share_name = "repartitioner-sharestate"
    envs = {
      "STATE_DIR" = "/app/state/current"
    }
    secrets = {}
    aca     = true
  },

  {
    name               = "soft2bet-funid"
    cpu                = 1
    memory             = 2
    docker_image_name  = "funid/player-processor"
    docker_image_tag   = "20251014"
    storage_share_name = "funid-sharestate"
    envs = {
      "STATE_DIR" = "/app/state/current"
    }
    secrets = {}
    aca     = true
  },
  {
    name               = "soft2bet-main"
    cpu                = 2
    memory             = 4
    docker_image_name  = "player-processor"
    docker_image_tag   = "20251016-metrics-7"
    storage_share_name = "main-sharestate"
    envs = {
      "STATE_DIR" = "/app/state/soft2bet-main"
    }
    secrets = {}
    aca     = true
  },


  {
    name               = "soft2bet-checker-anglet"
    cpu                = 2
    memory             = 4
    docker_image_name  = "checker/aggregators-api"
    docker_image_tag   = "20250929-choisy"
    storage_share_name = "checker-anglet-sharestate"
    envs = {
      "STATE_DIR" = "/app/state/current"
    }
    secrets = {}
    aca     = true
  },

  {
    name               = "soft2bet-checker-biarritz"
    cpu                = 2
    memory             = 4
    docker_image_name  = "checker/aggregators-api"
    docker_image_tag   = "20250929-choisy"
    storage_share_name = "checker-biarritz-sharestate"
    envs = {
      "STATE_DIR" = "/app/state/current"
    }
    secrets = {}
    aca     = true
  },




]

# Map of storage configurations to be created
storage_configs = {
  "logins-sharestate" = {
    quota = 5 # in GB
    aca   = true
  }

  "repartitioner-sharestate" = {
    quota = 5 # in GB
    aca   = true
  }


  "segmentation-sharestate" = {
    quota = 50 # in GB
    aca   = true
  }

  "statistics-sharestate" = {
    quota = 50 # in GB
    aca   = true
  }

  "firstdepositloss-sharestate" = {
    quota = 50 # in GB
    aca   = true
  }

  "deprecated-kpi-sharestate" = {
    quota = 50 # in GB
    aca   = true
  }

  "sportpush-sharestate" = {
    quota = 50 # in GB
    aca   = true
  }

  "ldc-2-sharestate" = {
    quota = 50 # in GB
    aca   = true
  }

  "main-sharestate" = {
    quota = 500 # in GB
    aca   = true
  }

  "funid-sharestate" = {
    quota = 50 # in GB
    aca   = true
  }

  "checker-anglet-sharestate" = {
    quota = 150 # in GB
    aca   = true
  }

  "checker-biarritz-sharestate" = {
    quota = 150 # in GB
    aca   = true
  }


}
