resource "azurerm_data_factory" "data_factory" {
  name                = "purviewatlaspoc-data-factory"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
}

resource "azurerm_data_factory_linked_service_data_lake_storage_gen2" "ls_storage_account" {
  name                  = "ls-storage-account"
  resource_group_name   = azurerm_resource_group.rg.name
  data_factory_name     = azurerm_data_factory.data_factory.name
  service_principal_id  = azuread_service_principal.sp.application_id
  service_principal_key = azuread_application_password.app_secret.value
  tenant                = azuread_service_principal.sp.application_tenant_id
  url                   = azurerm_storage_account.storage_account.primary_dfs_endpoint
}

