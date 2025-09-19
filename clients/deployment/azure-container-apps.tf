locals {

  aca                 = { for app in var.container_apps : app.name => app if app.aca }
  aca_storage_configs = { for key, config in var.storage_configs : key => config if config.aca }

}

resource "azurerm_subnet" "container-kafka" {
  count                = length(local.aca) > 0 ? 1 : 0
  name                 = var.subnet_name
  resource_group_name  = data.azurerm_resource_group.main.name
  virtual_network_name = var.virtual_network_name
  address_prefixes     = [var.address_prefixes]


  dynamic "delegation" {
    for_each = var.workload_profile != null ? [1] : []
    content {
      name = "container_app_delegation"
      service_delegation {
        name = "Microsoft.App/environments"
        actions = [
          "Microsoft.Network/virtualNetworks/subnets/join/action",
        ]
      }
    }
  }
}
# Create a Container App environment connected to the existing subnet
resource "azurerm_container_app_environment" "main" {
  count                      = length(local.aca) > 0 ? 1 : 0
  name                       = var.name
  location                   = data.azurerm_resource_group.main.location
  resource_group_name        = data.azurerm_resource_group.main.name
  infrastructure_subnet_id   = azurerm_subnet.container-kafka.0.id
  log_analytics_workspace_id = replace(data.azurerm_log_analytics_workspace.logs.id, "Monitoring", "monitoring")

  zone_redundancy_enabled        = false
  internal_load_balancer_enabled = false
  # Enable System Assigned Identity for the environment
  identity {
    type = "SystemAssigned"
  }

  tags = local.tags

  dynamic "workload_profile" {

    for_each = var.workload_profile != null ? [1] : []
    content {
      name                  = var.workload_profile
      workload_profile_type = var.workload_profile
    }
  }

}


resource "azurerm_container_app_environment_storage" "share_state" {
  for_each                     = local.aca_storage_configs
  name                         = each.key
  container_app_environment_id = azurerm_container_app_environment.main.0.id
  account_name                 = azurerm_storage_account.state.name
  access_key                   = azurerm_storage_account.state.primary_access_key
  share_name                   = azurerm_storage_share.state_share[each.key].name
  access_mode                  = "ReadWrite"
}



# Grant the Container App Environment's managed identity AcrPull permissions on the registry
resource "azurerm_role_assignment" "acr_pull_assignment" {
  count                = length(local.aca) > 0 ? 1 : 0
  scope                = data.azurerm_container_registry.acr.id
  role_definition_name = "AcrPull"
  principal_id         = azurerm_container_app_environment.main.0.identity[0].principal_id
}

# Create the Azure Container App itself
resource "azurerm_container_app" "main" {
  for_each                     = local.aca
  name                         = each.value.name
  resource_group_name          = data.azurerm_resource_group.main.name
  container_app_environment_id = azurerm_container_app_environment.main.0.id
  revision_mode                = "Multiple"

  # Container registry credentials to pull the image using managed identity
  registry {
    server   = data.azurerm_container_registry.acr.login_server
    identity = "system-environment"
  }

  secret {
    name  = "aws-secret-key"
    value = var.aws_secret_key_value
  }

  secret {
    name  = "storage-account-key"
    value = azurerm_storage_account.state.primary_access_key
  }

  secret {
    name  = "azure-account-key"
    value = var.azure_account_key
  }

  dynamic "secret" {
    for_each = each.value.datasource ? [1] : []
    content {
      name  = "sf-password"
      value = var.sf_password
    }
  }
  dynamic "secret" {
    for_each = each.value.datasource ? [1] : []
    content {
      name  = "datasource-passphrase"
      value = var.datasource_passphrase
    }
  }
  dynamic "secret" {
    for_each = each.value.datasource ? [1] : []
    content {
      name  = "datasource-pem-key-base64"
      value = var.datasource_pem_key_base64
    }
  }


  dynamic "secret" {
    for_each = each.value.eventhub ? [1] : []
    content {
      name  = "eventhub-namespace"
      value = var.eventhub_namespace
    }
  }

  dynamic "secret" {
    for_each = each.value.eventhub ? [1] : []
    content {
      name  = "shared-access-key"
      value = var.shared_access_key
    }
  }

  dynamic "secret" {
    for_each = each.value.eventhub ? [1] : []
    content {
      name  = "shared-access-key-name"
      value = var.shared_access_key_name
    }
  }

  dynamic "secret" {
    for_each = each.value.mks ? [1] : []
    content {
      name  = "mks-password"
      value = var.mks_password
    }
  }

  dynamic "secret" {
    for_each = each.value.mks ? [1] : []
    content {
      name  = "mks-username"
      value = var.mks_username
    }
  }


  dynamic "secret" {
    for_each = each.value.secrets
    content {
      name  = secret.key
      value = secret.value
    }
  }


  ingress {
    external_enabled = false
    target_port      = 8080
    traffic_weight {
      latest_revision = true
      percentage      = 100
    }
  }
  template {

    min_replicas = 1
    max_replicas = 1


    container {
      name   = "stream"
      image  = "${data.azurerm_container_registry.acr.login_server}/${each.value.docker_image_name}:${each.value.docker_image_tag}"
      cpu    = each.value.cpu
      memory = "${each.value.memory}Gi"
      env {
        name  = "AWS_ACCESS_KEY_ID"
        value = var.aws_access_key_id
      }

      env {
        name  = "AWS_DEFAULT_REGION"
        value = "eu-central-1"
      }
      env {
        name        = "AWS_SECRET_ACCESS_KEY"
        secret_name = "aws-secret-key"
      }
      env {
        name        = "AZURE_ACCOUNTKEY"
        secret_name = "azure-account-key"
      }

      dynamic "env" {
        for_each = each.value.envs
        content {
          name  = env.key
          value = env.value
        }
      }

      dynamic "env" {
        for_each = each.value.datasource ? [1] : []

        content {
          name        = "DATASOURCE_PASSPHRASE"
          secret_name = "datasource-passphrase"
        }
      }

      dynamic "env" {
        for_each = each.value.datasource ? [1] : []

        content {
          name        = "DATASOURCE_PEM_KEY_BASE64"
          secret_name = "datasource-pem-key-base64"
        }
      }


      dynamic "env" {
        for_each = each.value.datasource ? [1] : []
        content {
          name        = "SF_PASSWORD"
          secret_name = "sf-password"
        }
      }

      dynamic "env" {
        for_each = each.value.eventhub ? [1] : []
        content {
          name        = "EVENTHUB_NAMESPACE"
          secret_name = "eventhub-namespace"
        }
      }

      dynamic "env" {
        for_each = each.value.eventhub ? [1] : []
        content {
          name        = "SHARED_ACCESS_KEY"
          secret_name = "shared-access-key"
        }
      }

      dynamic "env" {
        for_each = each.value.eventhub ? [1] : []
        content {
          name        = "SHARED_ACCESS_KEY_NAME"
          secret_name = "shared-access-key-name"
        }
      }

      dynamic "env" {
        for_each = each.value.mks ? [1] : []
        content {
          name        = "MKS_PASSWORD"
          secret_name = "mks-password"
        }
      }

      dynamic "env" {
        for_each = each.value.mks ? [1] : []
        content {
          name        = "MKS_USERNAME"
          secret_name = "mks-username"
        }
      }



      dynamic "env" {
        for_each = each.value.secrets
        content {
          name        = upper(replace(env.key, "-", "_"))
          secret_name = env.key
        }
      }


      volume_mounts {
        name = "rocksdb-volume"
        path = "/app/state"
      }

      readiness_probe {
        transport = "HTTP"
        path      = "/q/health"
        port      = 8080
      }

      liveness_probe {
        transport = "HTTP"
        path      = "/q/health"
        port      = 8080
      }

    }

    volume {
      name         = "rocksdb-volume"
      storage_type = "AzureFile"
      storage_name = azurerm_container_app_environment_storage.share_state[each.value.storage_share_name].name
    }
  }
  tags = local.tags
}

resource "azurerm_monitor_metric_alert" "replica_count_alerts" {

  for_each = local.aca

  name                = "replica-count-alert-${each.key}"
  resource_group_name = data.azurerm_resource_group.main.name

  // The scopes are dynamically set for each app
  scopes = [azurerm_container_app.main[each.key].id]

  description = "Alerts on the number of replicas of the ${each.key} Container App."

  criteria {
    metric_namespace = "Microsoft.App/containerApps"
    metric_name      = "Replicas"
    aggregation      = "Maximum"
    operator         = "GreaterThan"
    threshold        = 1
  }

  action {
    action_group_id = azurerm_monitor_action_group.email_alert.id
  }

  window_size = "PT5M" // 5-minute look-back window
  frequency   = "PT1M" // Check every 1 minute
  tags        = local.tags
}

