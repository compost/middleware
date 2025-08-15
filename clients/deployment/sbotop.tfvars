# Existing Azure resources
name                 = "sbotop-environment"
client               = "sbotop"
resource_group_name  = "SBO-RG"
virtual_network_name = "SBO-vnet"
subnet_name          = "sbotop-environment"
address_prefixes     = "100.100.1.192/27"
workload_profile     = null
# New resources to be created
storage_account_name = "sbotopenvstorage" # Must be globally unique

# Environmental variables for your application
#aws_access_key_id    = ""
#aws_secret_key_value = ""


# List of container apps to be created
container_apps = [
  {
    name               = "sbotop-main"
    cpu                = 0.5
    memory             = 1
    docker_image_name  = "sbotop/sbotop"
    docker_image_tag   = "2025-08-08-2"
    storage_share_name = "sbotop-sharestate"
    envs = {
      "STATE_DIR"          = "/app/state/2025-08-26"
      "WEBSITE_DNS_SERVER" = "168.63.129.16"
    }
    secrets    = {}
    datasource = false
    aca        = false
  }

]

# Map of storage configurations to be created
storage_configs = {
  "sbotop-sharestate" = {
    quota = 50 # in GB
    aca   = false
  }
}
