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
    name               = "playbook-joy-main"
    cpu                = 0.5
    memory             = 1
    docker_image_name  = "playbook/playbook-stream"
    docker_image_tag   = "20250929-biarritz"
    storage_share_name = "playbook-joy-sharestate"
    envs = {
      "STATE_DIR"          = "/app/state/current"
      "WEBSITE_DNS_SERVER" = "168.63.129.16" # not sure
    }
    secrets    = {}

    aca = true
  },
  {
    name               = "playbook-balance"
    cpu                = 1
    memory             = 2
    docker_image_name  = "balance/playbook-stream"
    docker_image_tag   = "20250929-choisy"
    storage_share_name = "playbook-balance-sharestate"
    envs = {
      "STATE_DIR"          = "/app/state/current"
    }
    secrets    = {}

    aca = true
  }

]

storage_configs = {
  "playbook-joy-sharestate" = {
    quota = 50 
    aca   = true
  }

  "playbook-balance-sharestate" = {
    quota = 50
    aca   = true
  }
}
