# variables.tf

# Infrastructure naming and identification
variable "name" {
  description = "The name of the container app environment."
  type        = string
}

variable "alert_email_recipient" {
  description = "Email address to receive monitoring alerts."
  type        = string
  default     = "stephane.jeandeaux@tilelli.dev"
}

variable "workload_profile" {
  description = "Azure Container Apps workload profile type (e.g., Consumption, D4, D8)."
  type        = string
  nullable    = true
  default     = null
}

variable "create_outbound_ip" {
  description = "Whether to create a dedicated outbound IP for the ACA environment. Optional - used for AWS inbound rule configuration."
  type        = bool
  default     = false
}
variable "container_apps" {
  description = "A list of container apps to create."
  type = list(object({
    name               = string
    docker_image_name  = string
    docker_image_tag   = string
    storage_share_name = string
    cpu                = number
    memory             = number
    envs               = map(string)
    secrets            = map(string)
    datasource         = bool
    mks         = bool
    eventhub           = bool 
    aca                = bool
  }))
}

variable "storage_configs" {
  description = "A map of storage configurations."
  type = map(object({
    quota = number
    aca   = bool
  }))
}

variable "client" {
  description = "Client name for resource tagging and identification."
  type        = string
}
# Networking configuration
variable "address_prefixes" {
  description = "CIDR block for the subnet (e.g., '10.4.8.0/23')."
  type        = string
}
# Existing Azure resources
variable "resource_group_name" {
  description = "The name of the existing resource group where resources will be deployed."
  type        = string
}

variable "resource_group_name_of_registry" {
  description = "The name of the resource group containing the Azure Container Registry."
  type        = string
  default     = "centralised_DW"
}

variable "resource_group_name_of_monitoring" {
  description = "The name of the resource group containing the Log Analytics workspace."
  type        = string
  default     = "monitoring"
}
variable "virtual_network_name" {
  description = "The name of the existing virtual network."
  type        = string
}

variable "subnet_name" {
  description = "The name of the existing subnet."
  type        = string
}

variable "container_registry_name" {
  description = "The name of the existing Azure Container Registry."
  type        = string
  default     = "centralregistry"
}

variable "log_analytics_workspace_name" {
  description = "The name of the existing Log Analytics Workspace."
  type        = string
  default     = "Monitoring"
}

# New resources
variable "storage_account_name" {
  description = "The name of the storage account for RocksDB state (must be globally unique)."
  type        = string
}

variable "azure_account_key" {
  description = "Azure storage account access key for application use."
  type        = string
  sensitive   = true
}
# Application secrets and credentials
variable "aws_access_key_id" {
  description = "AWS access key ID for SQS integration."
  type        = string
  sensitive   = true
}

variable "aws_secret_key_value" {
  description = "AWS secret access key for SQS integration."
  type        = string
  sensitive   = true
}

variable "tags" {
  description = "A mapping of tags to assign to the resource."
  type        = map(string)
  default = {
    "Team"      = "Middleware"
    "ManagedBy" = "terraform"
    "Cost"      = "middleware-2"
  }
}

# Snowflake database credentials
variable "sf_password" {
  description = "Snowflake database password for data source connections."
  type        = string
  sensitive   = true
}

variable "datasource_passphrase" {
  description = "Passphrase for datasource PEM key decryption."
  type        = string
  sensitive   = true
}

variable "datasource_pem_key_base64" {
  description = "Base64-encoded PEM key for secure datasource connections."
  type        = string
  sensitive   = true
}

# Azure Event Hub credentials
variable "eventhub_namespace" {
  description = "Azure Event Hub namespace for event streaming."
  type        = string
  sensitive   = true
}
variable "shared_access_key" {
  description = "Azure Event Hub shared access key."
  type        = string
  sensitive   = true
}
variable "shared_access_key_name" {
  description = "Azure Event Hub shared access key name."
  type        = string
  sensitive   = true
}

variable "mks_username" {
  description = "MKS username."
  type        = string
  sensitive   = true
}
variable "mks_password" {
  description = "MKS password."
  type        = string
  sensitive   = true
}
