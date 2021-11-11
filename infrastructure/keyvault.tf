resource "azurerm_key_vault" "keyvault" {
  name                        = "kvpurviewatlaspoc"
  location                    = azurerm_resource_group.rg.location
  resource_group_name         = azurerm_resource_group.rg.name
  enabled_for_disk_encryption = true
  tenant_id                   = data.azurerm_client_config.current.tenant_id
  soft_delete_retention_days  = 7
  purge_protection_enabled    = false

  sku_name = "standard"

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id

    key_permissions = [
      "Get",
    ]

    secret_permissions = [
      "Get", "List", "Purge", "Delete", "Set"
    ]

  }

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = azuread_application.app.object_id

    key_permissions = [
      "Get",
    ]

    secret_permissions = [
      "Get",
    ]
  }

  access_policy {
    tenant_id = azurerm_data_factory.data_factory.identity[0].tenant_id
    object_id = azurerm_data_factory.data_factory.identity[0].principal_id

    key_permissions = [
      "Get",
    ]

    secret_permissions = [
      "Get", "List", "Delete", "Set"
    ]
  }
}

resource "azurerm_key_vault_secret" "client_secret" {
  name         = "client-secret"
  value        = azuread_application_password.app_secret.value
  key_vault_id = azurerm_key_vault.keyvault.id
}
