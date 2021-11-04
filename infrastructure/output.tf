output "client_id" {
  value = azuread_application.app.application_id
}

output "tenant_id" {
  value = azuread_service_principal.sp.application_tenant_id
}

output "client_secret" {
  value     = azuread_application_password.app_secret.value
  sensitive = true
}

output "atlas_endpoint" {
  value = azurerm_purview_account.pv_account.catalog_endpoint
}
