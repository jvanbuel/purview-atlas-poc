resource "azurerm_data_factory" "data_factory" {
  depends_on = [
    azurerm_storage_blob.country_data, azurerm_storage_blob.vaccine_data, azurerm_purview_account.pv_account
  ]
  name                = "purviewatlaspoc-data-factory"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name

  github_configuration {
    account_name    = "jvanbuel"
    branch_name     = "main"
    git_url         = "https://github.com"
    repository_name = "purview-atlas-poc"
    root_folder     = "/data_factory"
  }

  identity {
    type = "SystemAssigned"
  }
}




