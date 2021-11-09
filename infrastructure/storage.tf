
resource "azurerm_storage_account" "storage_account" {
  name                     = "purviewatlaspoc"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "GRS"
  is_hns_enabled           = true
}


resource "azurerm_storage_container" "datalake" {
  name                  = "datalake"
  storage_account_name  = azurerm_storage_account.storage_account.name
  container_access_type = "private"
}

resource "azurerm_role_assignment" "role" {
  scope                = azurerm_storage_account.storage_account.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azuread_service_principal.sp.object_id
}