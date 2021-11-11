terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~>2.83.0"
    }
    azuread = {
      source = "hashicorp/azuread"
      version = "2.8.0"
    }
  }
}

provider "azurerm" {
  # Configuration options
  features {

  }
}

provider "azuread" {
  # Configuration options
}

data "azuread_client_config" "current" {}
data "azurerm_client_config" "current" {}