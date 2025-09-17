# Define the required providers and their versions
terraform {
  backend "local" {
  }
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.40.0"
    }
  }
}

# Configure the AzureRM provider
provider "azurerm" {
  resource_provider_registrations = "none"
  features {}
}

# Local values for consistent resource tagging
locals {
  tags = merge({
    "Client" = var.client
  }, var.tags)
}

# Data sources to reference existing Azure resources
data "azurerm_resource_group" "main" {
  name = var.resource_group_name
}

data "azurerm_resource_group" "registry" {
  name = var.resource_group_name_of_registry
}

data "azurerm_resource_group" "monitoring" {
  name = var.resource_group_name_of_monitoring
}

# Data source to reference the existing Azure Container Registry
data "azurerm_container_registry" "acr" {
  name                = var.container_registry_name
  resource_group_name = data.azurerm_resource_group.registry.name
}

# Data source to reference the existing Log Analytics Workspace
data "azurerm_log_analytics_workspace" "logs" {
  name                = var.log_analytics_workspace_name
  resource_group_name = data.azurerm_resource_group.monitoring.name
}

# Create a storage account for the persistent volume
resource "azurerm_storage_account" "state" {
  name                     = var.storage_account_name
  resource_group_name      = data.azurerm_resource_group.main.name
  location                 = data.azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  tags                     = local.tags
}

# Create an Azure Files share to be used as the persistent volume
resource "azurerm_storage_share" "state_share" {
  for_each           = var.storage_configs
  name               = each.key
  storage_account_id = azurerm_storage_account.state.id
  quota              = each.value.quota
}

# Monitoring and alerting configuration
resource "azurerm_monitor_action_group" "email_alert" {
  name                = "email-action-group"
  resource_group_name = data.azurerm_resource_group.main.name
  short_name          = "email-alert"

  email_receiver {
    name          = "email_recipient"
    email_address = var.alert_email_recipient
  }

  tags = local.tags
}

resource "azurerm_monitor_metric_alert" "storage_share_usage_alerts" {
  name                = "storage-usage-alert"
  resource_group_name = data.azurerm_resource_group.main.name
  scopes              = [azurerm_storage_account.state.id]
  description         = "Alert when storage usage exceeds 50% of total quota."

  criteria {
    metric_namespace = "Microsoft.Storage/storageAccounts"
    metric_name      = "UsedCapacity"
    aggregation      = "Average"
    operator         = "GreaterThan"

    threshold = (sum([for s in var.storage_configs : s.quota]) * 1024 * 1024 * 1024) * 0.50
  }

  action {
    action_group_id = azurerm_monitor_action_group.email_alert.id
  }

  window_size = "PT6H"
  frequency   = "PT5M"
  tags        = local.tags

}
