## In progress to not use
locals {
  # This map correctly filters for container apps that are NOT ACA.
  # The 'name' is used as the key for the for_each loop.
  aci = { for app in var.container_apps : app.name => app if !app.aca }
  # This map correctly filters storage configurations that are NOT ACA.
  aci_storage_configs = { for key, config in var.storage_configs : key => config if !config.aca }
}

resource "azurerm_subnet" "container-kafka-aci" {
  count                = length(local.aci) > 0 ? 1 : 0
  name                 = var.subnet_name
  resource_group_name  = data.azurerm_resource_group.main.name
  virtual_network_name = var.virtual_network_name
  address_prefixes     = [var.address_prefixes]

  delegation {
    name = "ACIDelegationService"
    service_delegation {
      name = "Microsoft.ContainerInstance/containerGroups"
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/action",
      ]
    }
  }
}


# Create the Azure Container Group instead of a Container App Environment
resource "azurerm_container_group" "main" {
  for_each            = local.aci
  name                = each.value.name
  location            = data.azurerm_resource_group.main.location
  resource_group_name = data.azurerm_resource_group.main.name

  diagnostics {
    log_analytics {
      log_type      = "ContainerInsights"
      metadata      = null
      workspace_id  = data.azurerm_log_analytics_workspace.logs.workspace_id
      workspace_key = data.azurerm_log_analytics_workspace.logs.primary_shared_key
    }
  }

  image_registry_credential {
    server   = data.azurerm_container_registry.acr.login_server
    username = data.azurerm_container_registry.acr.admin_username
    password = data.azurerm_container_registry.acr.admin_password
  }

  ip_address_type = "Private"
  os_type         = "Linux"

  # The 'subnet_ids' property expects a list of subnet IDs, so the existing list syntax is correct.
  subnet_ids = [azurerm_subnet.container-kafka-aci.0.id]

  restart_policy = "OnFailure"

  # Container definition within the container group
  container {
    name   = each.value.name
    image  = "${data.azurerm_container_registry.acr.login_server}/${each.value.docker_image_name}:${each.value.docker_image_tag}"
    cpu    = each.value.cpu
    memory = each.value.memory
    ports {
      port     = 8080
      protocol = "TCP"
    }

    environment_variables = merge({
      "AWS_DEFAULT_REGION" : "eu-central-1"
      "AWS_ACCESS_KEY_ID" : var.aws_access_key_id
    }, each.value.envs)

    secure_environment_variables = merge({
      AWS_SECRET_ACCESS_KEY : var.aws_secret_key_value
      AZURE_ACCOUNTKEY : var.azure_account_key

      }, each.value.datasource ? {
      DATASOURCE_PASSPHRASE : var.datasource_passphrase
      DATASOURCE_PEM_KEY_BASE64 : var.datasource_pem_key_base64
      SF_PASSWORD : var.sf_password
    } : {}, each.value.secrets)

    liveness_probe {

      initial_delay_seconds = 180
      failure_threshold  = 5
      timeout_seconds  = 60
      period_seconds = 300
      http_get {
        scheme = "http"
        path   = "/q/health"
        port   = 8080

      }

    }

    volume {
      name                 = "rocksdb-volume"
      storage_account_name = azurerm_storage_account.state.name
      storage_account_key  = azurerm_storage_account.state.primary_access_key
      share_name           = azurerm_storage_share.state_share[each.value.storage_share_name].name
      mount_path           = "/app/state"
    }
  }

  tags = local.tags
}

resource "azurerm_monitor_activity_log_alert" "aci_restart_alert" {

  for_each            = local.aci
  name                = "aci-restart-alert-${each.key}"
  resource_group_name = data.azurerm_resource_group.main.name
  location            = data.azurerm_resource_group.main.location
  scopes              = [azurerm_container_group.main[each.key].id]
  description         = "Alerts when the container group ${each.key} is restarted."

  criteria {
    operation_name = "Microsoft.ContainerInstance/containerGroups/restart/action"
    category       = "Administrative"
  }

  action {
    action_group_id = azurerm_monitor_action_group.email_alert.id
  }

  tags = local.tags

}
