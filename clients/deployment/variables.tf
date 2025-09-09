# variables.tf

# Existing resources
variable "name" {
  description = "The name of the existing resource group."
  type        = string
}

variable "alert_email_recipient" {
  description = "The name of the existing resource group."
  type        = string
  default     = "stephane.jeandeaux@tilelli.dev"
}

variable "workload_profile" {
  description = "The name of the existing resource group."
  type        = string
  nullable    = true
  default     = null
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
  description = "The name of the existing resource group."
  type        = string
}
variable "address_prefixes" {
  description = "The name of the existing resource group."
  type        = string
}
variable "resource_group_name" {
  description = "The name of the existing resource group."
  type        = string
}

variable "resource_group_name_of_registry" {
  description = "The name of the existing resource group."
  type        = string
  default     = "centralised_DW"
}

variable "resource_group_name_of_monitoring" {
  description = "The name of the existing resource group."
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
  description = "The AWS access key ID for the application."
  type        = string
  sensitive   = true # Mark this as sensitive
}
# Environmental variables
variable "aws_access_key_id" {
  description = "The AWS access key ID for the application."
  type        = string
  sensitive   = true # Mark this as sensitive
}

variable "aws_secret_key_value" {
  description = "The AWS secret access key."
  type        = string
  sensitive   = true # Mark this as sensitive
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

variable "sf_password" {
  type      = string
  sensitive = true
}

variable "datasource_passphrase" {
  type      = string
  sensitive = true
}

variable "datasource_pem_key_base64" {
  type      = string
  sensitive = true
}

variable "eventhub_namespace" {
  type      = string
  sensitive = true
}
variable "shared_access_key" {
  type      = string
  sensitive = true
}
variable "shared_access_key_name" {
  type      = string
  sensitive = true
}
