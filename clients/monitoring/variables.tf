# Email configuration
variable "alert_email_recipient" {
  description = "Email address to receive monitoring alerts."
  type        = string
  default     = "stephane.jeandeaux@tilelli.dev"
}

# Tags of the application
variable "tags" {
  description = "A mapping of tags to assign to the resource."
  type        = map(string)
  default = {
    "Team"      = "Middleware"
    "ManagedBy" = "terraform"
    "Cost"      = "middleware-2"
  }
}

# Existing Azure resource : Monitoring
variable "resource_group_name_of_monitoring" {
  description = "The name of the resource group containing the Log Analytics workspace."
  type        = string
  default     = "Monitoring"
}

# Existing Azure resource : Monitoring -> Logs
variable "log_analytics_workspace_name" {
  description = "The name of the existing Log Analytics Workspace."
  type        = string
  default     = "Monitoring"
}