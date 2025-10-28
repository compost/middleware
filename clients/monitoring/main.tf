# Define the required providers and their versions
terraform {

  required_version = ">= 1.0"
  
  backend "s3" {
    bucket  = "symplify-terraform-state-bucket"     
    key     = "terraform.tfstate"          
    region  = "eu-central-1"                         
    encrypt = true
    
  }
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.14.1"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.40.0"
    }
  }
}

# Configure the AzureRM provider
provider "azurerm" {
  features {}
}

# Local values for consistent resource tagging
locals {
  tags = merge({
    "Client" = "All" 
  }, var.tags)
}

# Data source to reference the existing "Monitoring" workspace
data "azurerm_resource_group" "monitoring" {
  name = var.resource_group_name_of_monitoring
}

# Data source to reference the existing Log Analytics Workspace
data "azurerm_log_analytics_workspace" "logs" {
  name                = var.log_analytics_workspace_name
  resource_group_name = data.azurerm_resource_group.monitoring.name
}

# Monitoring and alerting configuration
resource "azurerm_monitor_action_group" "email_alert" {
  name                = "email-action-group"
  resource_group_name = data.azurerm_resource_group.monitoring.name
  short_name          = "email-alert"

  email_receiver {
    name          = "email_recipient"
    email_address = var.alert_email_recipient
  }

  tags = local.tags
}

# AzureRM alert configurations on error logs
resource "azurerm_monitor_scheduled_query_rules_alert_v2" "error_logs_alert" {
  name                 = "error-logs-alert"
  display_name         = "error-logs-alert"
  location             = data.azurerm_resource_group.monitoring.location
  resource_group_name  = data.azurerm_resource_group.monitoring.name
  description          = "Alert when ACA returns log erros"

  scopes               = [data.azurerm_log_analytics_workspace.logs.id] 

  criteria {
    query                   = <<-QUERY
        ContainerAppConsoleLogs_CL
        | where parse_json(Log_s).["log.level"] == "ERROR"
      QUERY
    time_aggregation_method = "Count" 
    operator                = "GreaterThan"
    threshold               = 0  
  }

  auto_mitigation_enabled          = false
  workspace_alerts_storage_enabled = false
  enabled                          = true
  query_time_range_override        = "PT15M" 
  skip_query_validation            = true
  window_duration                  = "PT5M"
  evaluation_frequency             = "PT15M"
  severity                         = 3  

  action {
    action_groups = [azurerm_monitor_action_group.email_alert.id]
  }

  tags = local.tags
}