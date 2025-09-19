# outputs.tf

output "aca_outbound_ip_address" {
  description = "The outbound IP address of the ACA environment for AWS security group configuration"
  value       = var.create_outbound_ip && length(local.aca) > 0 ? azurerm_public_ip.aca_outbound[0].ip_address : null
}