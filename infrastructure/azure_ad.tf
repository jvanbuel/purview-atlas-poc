data "azuread_client_config" "current" {}

resource "azuread_application" "app" {
  display_name = "purviewatlaspoc"
  owners       = [data.azuread_client_config.current.object_id]
}

resource "azuread_service_principal" "sp" {
  application_id               = azuread_application.app.application_id
  app_role_assignment_required = false
  owners                       = [data.azuread_client_config.current.object_id]
}

resource "time_rotating" "secret_rotation" {
  rotation_days = 7
}

resource "azuread_application_password" "app_secret" {
  application_object_id = azuread_application.app.object_id
  rotate_when_changed = {
    rotation = time_rotating.secret_rotation.id
  }
}