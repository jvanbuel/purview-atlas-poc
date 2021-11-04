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