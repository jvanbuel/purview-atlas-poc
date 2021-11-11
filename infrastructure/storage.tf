
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

resource "azurerm_role_assignment" "purview_role" {
  scope                = azurerm_storage_account.storage_account.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_purview_account.pv_account.identity[0].principal_id
}

resource "azurerm_role_assignment" "df_role" {
  scope                = azurerm_storage_account.storage_account.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.data_factory.identity[0].principal_id
}

resource "azurerm_storage_blob" "country_data" {
  name                   = "raw/country_data.csv"
  storage_account_name   = azurerm_storage_account.storage_account.name
  storage_container_name = azurerm_storage_container.datalake.name
  type                   = "Block"
  source                 = "${path.root}/../data/country_data.csv"
}

resource "azurerm_storage_blob" "vaccine_data" {
  name                   = "raw/vaccine_data.csv"
  storage_account_name   = azurerm_storage_account.storage_account.name
  storage_container_name = azurerm_storage_container.datalake.name
  type                   = "Block"
  source                 = "${path.root}/../data/vaccine_data.csv"
}